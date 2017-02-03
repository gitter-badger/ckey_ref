# -*- coding: utf-8 -*-

# Filename : jcf.py
# Date & Author : 2013-11-20, Chris Nelson
# Platform: CentOS 6.2, Python 2.6.6
# Requirement: none
# Revision: 1
# (c) 2013 Hewlett-Packard Company. All Rights Reserved

import util
import json
from os.path import isfile, dirname, basename, join, abspath
from pprint import pprint, pformat
from copy import copy, deepcopy
import re
import random
import time
import logging
from __builtin__ import True

# Custom exceptions used internally for this class
class FlowError(Exception):
    def __init__(self, message):
        self.message = message


''' jcf class
implements core
JCF represents a Job Configuration File -- the raw data without any special
decoding of objects. It is responsible for processing a JCF to merge includes,
replace system variables, and ckeys.

Usage:
jcf = JCF(file)
  or
jcf = JCF(job_data)

Second form is to support the Job subclass which may feed in pre-parsed json
data.

To process includes, variables, etc.
jcf.process()

And to save results to same file or a new file
jcf.save()
  or
jcf.save(new_file)
'''


class JCF(object):
    # Sections - this list contains all sections (class members) that should be
    # exported to JSON, it also serves to check if any of these keys are not
    # recognized (this will cause a ValueError)
    section_members = [
        "info",
        "init_stage",
        "tags",
        "include",
        "included",
        "local",
        "suts",
        "stages",
        "ckey",
        "local_ckey",
        "ckey_template",
        "origination",
        "module_info",
        "dynamic",
        "job_timeout",
        "include_options",
        "is_global",
        "configure",
        "optionTextMap",
        "id",
        "job_group"
    ]

    # Flow Controls - this is a list of all valid flow controls
    flow_controls = [
        "next_default",
        "next_pass",
        "next_fail",
        "next_timeout"
    ]

    # Flow Control Values - this is a list of all valid special flow control values
    flow_control_values = [
        "_next",
        "_quit"
    ]

    def __init__(self, json_src={}, max_depth=100, serial=None, default_owner=None):
        # Internal
        self._raw = None
        self._serial = serial
        self.auto_init_stage = None

        # Members that represent a section within a JCF
        self.info = None
        self.init_stage = None
        self.tags = None
        self.include = None
        self.included = None
        self.local = None
        self.suts = None
        self.stages = None
        self.ckey = None
        self.local_ckey = None
        self.ckey_template = None
        self.origination = None
        self.module_info = None
        self.dynamic = None
        self.job_timeout = None
        self.include_options = None
        self.is_global = None
        self.configure = None
        self.optionTextMap = None
        self.id = None
        self.job_group = None
        # Members that represent other data
        self.path = None
        self.templates = None
        self.max_depth = max_depth
        self.order = 1
        self.interpolation_errors = list()
        self.default_name = "cirrus_job"

        if isinstance(json_src, dict):
            # Preparsed JSON
            raw = deepcopy(json_src)
        elif isinstance(json_src, list):
            # Lines from a file
            # Combine into single string and then make into JSON
            json_src = "\n".join(json_src)
            raw = util.read_json_str(json_src)
        else:
            # A file name
            # This will search for the file and add the .json suffix if needed
            self.path = util.retrieve_file(json_src, exts=[".json"])

            raw = util.read_json(self.path)

        # Create a default name
        if self.path:
            default_name = basename(self.path)
            if default_name.endswith(".json"):
                default_name = default_name[:-5]
        else:
            default_name = self.default_name

        # Set defaults for all members
        self.info = raw.get("info", dict())
        self.init_stage = raw.get("init_stage", None)
        self.tags = raw.get("tags", [])
        self.include = raw.get("include", {})
        self.included = raw.get("included", [])
        self.local = raw.get("local", dict())
        self.tags = raw.get("tags", [])
        self.suts = raw.get("suts", dict())
        self.stages = raw.get("stages", {})
        self.ckey = raw.get("ckey", dict())
        self.local_ckey = raw.get("local_ckey", dict())
        self.ckey_template = raw.get("ckey_template", dict())
        self.module_info = raw.get("module_info", dict())
        self.dynamic = raw.get("dynamic", dict())
        self.origination = raw.get("origination", None)
        self.job_timeout = raw.get("job_timeout", None)
        self.configure = raw.get("configure", None)
        self.optionTextMap = raw.get("optionTextMap", None)
        self.include_options = raw.get("include_options", None)
        self.id = raw.get("id", None)
        self.job_group = raw.get("job_group", [])
        self._raw = raw
        self.is_global = raw.get("is_global", dict())

        # Import or create serial
        if "info" in raw and "_serial" in raw["info"]:
            self._serial = raw["info"]["_serial"]
            self.info["_serial"] = self._serial
        elif self._serial:
            raw["info"]["_serial"] = self._serial
            self.info["_serial"] = self._serial
        else:
            self._serial = default_name + "_" + \
                str(random.randint(1000000000, 4000000000))
            if "info" not in raw:
                raw["info"] = dict()
            raw["info"]["_serial"] = self._serial
            self.info["_serial"] = self._serial

        # Store local information
        if self._serial not in self.local:
            self.local[self._serial] = dict()
        # Move local ckeys to local area and delete originals
        self.local[self._serial].update(self.local_ckey)
        self.local_ckey = dict()

        # Backward compatibility check for name, desc
        if "name" in raw:
            self.info["name"] = raw["name"]
            del raw["name"]
        if "desc" in raw:
            self.info["desc"] = raw["desc"]
            del raw["desc"]

        # Set default info section content if not present
        if "name" not in self.info:
            self.info["name"] = default_name
        if "desc" not in self.info:
            self.info["desc"] = ''
        if "login_name" not in self.info:
            self.info["login_name"] = default_owner if default_owner is not None else "anonymous"

        # Sanity checks
        # Are there unrecognized sections?
        unrecognized = []
        for k in raw.keys():
            if k not in self.section_members:
                unrecognized.append(k)
        if unrecognized:
            raise ValueError("JCF " + str(self.path) +
                             " contains unrecognized sections: " + ",".join(unrecognized))

        # Extract template names from includes. Do not do this recursively
        # because at this time we just want to know if the immediate descendants
        # are templates or not.
        if self.include:
            if self.path:
                include_paths = [dirname(self.path), "."]
            else:
                include_paths = ["."]
            self.templates = []
            if self.max_depth:
                for n in range(len(self.include)):
                    # Handle simple include format
                    if isinstance(self.include[n], str) or isinstance(self.include[n], unicode):
                        include_file = self.include[n]
                        self.include[n] = {
                            "id": include_file
                        }
                    else:
                        if "id" in self.include[n]:
                            include_file = self.include[n]["id"]
                        else:
                            raise ValueError("include structure is missing 'id' field")

                    # Create temporary JCF object, depth to 0 to avoid
                    # recursion
                    try:
                        local_file = util.retrieve_file(include_file,
                                                        include_paths,
                                                        [".json"],
                                                        remote=False)
                        included_jcf = JCF(local_file, max_depth=0)
                        if "template" in included_jcf.tags:
                            self.templates.append(included_jcf.path)
                    except ValueError, IOError:
                        # Skip any that we cannot read
                        pass

        # Set origination for this JCF--the local IP
        # TODO: this will work for current activities which mainly validate that
        # the current system is the one we think it is for stage execution
        # (code in agent.py) but this needs to be refined to identify primary
        # IPs in multi-homed hosts.
        # See also: sys_cirrus setting
        if not self.origination:
            self.origination = unicode(util.get_preferred_local_ip())

        # Propagate serial numbers to stages
        stage_list = list()
        if isinstance(self.stages, dict):
            stage_list = self.stages.values()
        elif isinstance(self.stages, list):
            for s in self.stages:
                if isinstance(s, dict):
                    stage_list.extend(s.values())
                else:
                    raise ValueError("JCF " + str(self.path) +
                                     " stages section is corrupt, each list " +
                                     " element must contain exactly one stage")
        for s in stage_list:
            if "_serial" not in s or not s["_serial"]:
                s["_serial"] = self._serial

    def __str__(self):
        jcfstr = "<JCF> " + str(self.path) + "\n"
        return jcfstr

    def print_jcf(self, indent=0):
        s = " " * indent
        print s + "JCF: " + str(self.path)
        print s + "    name: " + self.info["name"]
        print s + "    desc: " + self.info["desc"]
        print s + "    serial id: " + self._serial
        print s + "    origination: " + self.origination
        print s + "    tags: " + pformat(self.tags)
        print s + "    include: " + pformat(self.include)
        print s + "    included: " + pformat(self.included)

        if self.info:
            print "    info:"
            pprint(self.info, indent=8 + indent)
        if self.job_timeout:
            print "    job_timeout:", self.job_timeout
        if self.configure:
            print "    configure:"
            pprint(self.configure, indent=8 + indent)
        if self.suts:
            print "    suts:"
            pprint(self.suts, indent=8 + indent)
        if self.stages:
            print "    stages:"
            pprint(self.stages, indent=8 + indent)
        if self.ckey:
            print "    ckey:"
            pprint(self.ckey, indent=8 + indent)
        if self.local:
            print "    local:"
            pprint(self.local, indent=8 + indent)
        if self.ckey_template:
            print "    ckey_template:"
            pprint(self.ckey_template, indent=8 + indent)
        if self.module_info:
            print "    module_info:"
            pprint(self.module_info, indent=8 + indent)
        if self.dynamic:
            print "    dynamic:"
            pprint(self.dynamic, indent=8 + indent)
        if self.job_group:
            print "    job_group:"
            pprint(self.job_group, indent=8 + indent)
        print

    def print_raw(self):
        print util.write_json_pretty_str(self.get_raw())

    def print_sequence(self):
        print "File serial:", self._serial
        os = self.get_ordered_stages()
        for id, data in os:
            print "{0:20} P->{1:20} F->{2:20} ({3})".format(id,
                                                data.get("next_pass", "-"),
                                                data.get("next_fail", "-"),
                                                data.get("next_default", "-"))

    def process(self, recursive=True):
        '''
        Process all elements of a JCF so that it is in it's final form
        '''

        self.process_stages()
        self.process_includes()
        self.process_stages()
        self.process_system_vars()
        self.process_file_vars()

    def create_stage_specific_jcf_file(self, stage=None, filename="job.json"):
        self.process_stage_vars(stage)

        # Process JCF system data
        self.process_system_vars()

        # Create modified job control file in working area
        # Accepts linux or windows or "c:\" type filenames
        if filename and (filename.startswith("/") or
                         filename.startswith("\\") or
                         (len(filename) > 1 and filename[1] == ":")):
            new_jcf = filename
        elif self.path:
            new_jcf = join(dirname(self.path), filename)
        else:
            raise ValueError("filename must be a full path or JCF object " +
                             "must be created from a file originally so " +
                             "path member is set")
        self.write(new_jcf)

        # Update path
        self.path = new_jcf

    def process_includes(self, depth=0):
        '''
        !!!!!!!!!!!!!!!!!!!!!!!!!!!!
        IF YOU CHANGE THIS METHOD BE
        SURE TO UPDATE THE PARALLEL
        METHOD IN JOB.PY IF NEEDED
        !!!!!!!!!!!!!!!!!!!!!!!!!!!!

        Merges included JCFs

        max_depth - indicates how deep to read the tree. Default is a high
        number to catch circular includes.
           -1 = process to infinite depth
            0 = do not process includes (makes this method a noop)
           >0 = process to the indicated depth
        '''

        # TODO: detect circular includes or just rely on the max_depth setting?
        # Check if depth limit has been reached
        if self.max_depth != -1 and depth >= self.max_depth:
            return

        # Nothing to include
        if self.include is None:
            return

        all_serials = []
        if self.stages and type(self.stages) is dict:
            all_serials = [v["_serial"] for v in self.stages.values() if self.stages\
                       and "_serial" in v]
        all_serials.append(self.info["_serial"])

        if len(self.include) is 0 and self.ckey_template:
            contained = set(self.ckey_template.keys()) - set(all_serials)
            if len(contained) > 0:
                if self.stages is None or len(self.stages) is 0:
                    serial_name = "_nostage_" + self.info["_serial"]
                else:
                    serial_name = self.info["_serial"]
                x = deepcopy(self.ckey_template)
                self.ckey_template = { serial_name: x }

        # Stages must be in dict format
        if self.stages and isinstance(self.stages, list):
            self.process_stages()

        if self.path:
            include_paths = [dirname(self.path), "."]
        else:
            include_paths = ["."]

        for n in range(len(self.include)):
            serial = None
            status = None
            # Handle simple include format
            if isinstance(self.include[n], str) or isinstance(self.include[n], unicode):
                include_file = self.include[n]
                self.include[n] = {
                    "id": include_file
                }
            else:
                if "id" in self.include[n]:
                    include_file = self.include[n]["id"]
                else:
                    raise ValueError("include structure is missing 'id' field")
                if "_serial" in self.include[n]:
                    serial = self.include[n]["_serial"]
                if "status" in self.include[n]:
                    status = self.include[n]["status"]

            local_file = util.retrieve_file(include_file, include_paths, [".json"])
            merge_from = JCF(local_file,
                             serial=serial)
            merge_from.process_stages()

            # Add to included with full path for reference purposes
            self.included.append(local_file)

            # Call recursive merge
            merge_from.process_includes(depth=depth + 1)

            # Set flow control for this set of stages
            if self.include_options and \
                    merge_from._serial in self.include_options:
                io = self.include_options[merge_from._serial]

                last_stage_of_current = self.get_last_stage()
                first_stage_of_merge = merge_from.get_first_stage()

               # Tie the last stage of the current file to the first stage
                # of the next file
                if last_stage_of_current and first_stage_of_merge:
                    last_stage_of_current[1]["next_default"] = \
                        first_stage_of_merge[0]

            # Merge all sections
            self.merge(merge_from)

            # Add to included line for reference
            self.included = self.included + merge_from.included

        # Uniquify included line
        self.included = list(set(self.included))

        # Empty include line to prevent future merging
        self.include = []

    def merge(self, merge_from):
        '''
        Merges the merge_from JCF object into this one. Merge is done
        on each individual section separately depending on the structure for
        that particular section.
        '''
        # Merge Sections
        #     tags
        #     include (implicitly done via process_includes())
        #     info
        #     suts
        #     stages
        #     ckey
        #     local
        #     ckey_template
        #     job_timeout
        #     job_group

        # Create return data structure
        change_data = dict()

        # Path
        # Inherit path from next JCF if not set. This is to ensure that pathless
        # JCFs return semi-significant error strings.
        if not self.path and merge_from.path:
            self.path = merge_from.path

        # Tags
        # Uniquely merge the tag lists
        self.tags = sorted(list(set(self.tags + merge_from.tags)))

        # Info
        # This probably needs better handling but for now just take the info section
        # that contains a cirrus_job_id, else merge key-by-key
        if "cirrus_job_id" not in self.info and "cirrus_job_id" in merge_from.info:
            # merge_from merges into self
            self.info.update(merge_from.info)
        else:
            # self merges info merge_from
            # merge_from becomes new info section
            x = deepcopy(merge_from.info)
            x.update(self.info)
            self.info = x

        # special case for "name" -- take first one that is not the default
        # and pull in "desc" from that as well
        if self.info["name"] == self.default_name and \
        merge_from.info["name"] != self.default_name:
            for k in ("name", "desc"):
                if k in merge_from.info and merge_from.info[k]:
                    self.info[k] = merge_from.info[k]

        # Suts
        # suts that have the same ID are renamed and references updated much
        # the same as stages
#        x = deepcopy(merge_from.suts)
#        x.update(self.suts)
#        self.suts = x
        my_sut_names = self.suts.keys()
        merge_from_sut_names = merge_from.suts.keys()
        for n in merge_from_sut_names:
            new_name = n
            if n in my_sut_names:
                # Begin rename process
                # Create a new name
                i = 2
                new_name = n + "_" + str(i)
                while new_name in my_sut_names or new_name in merge_from_sut_names:
                    i += 1
                    new_name = n + "_" + str(i)

                # Move sut to new name
                merge_from.suts[new_name] = merge_from.suts[n]
                del merge_from.suts[n]

                # Find and replace all variable references to this sut
                var_re = r'\$\{suts\.(' + n + r')[-\w\s\. \[\]]+\}'
                util.find_replace_re(merge_from.get_dict(), var_re, new_name)
                merge_from.update_attributes()

                # Find and replace all target controls referencing this sut
                for stage_data in merge_from.stages.values():
                    if "target" in stage_data and stage_data["target"] == n:
                        stage_data["target"] = new_name

            # Merge in new sut
            self.suts[new_name] = merge_from.suts[new_name]



        # Stages
        # TODO: need to determine how to best handle conflicts
        #   Options:
        #    1) replace with current version (consistent with other operations)
        #    2) replace with merge version
        #    3) rename duplicates with a new identifier
        # -- I like #3 because stages like "reboot" could be called multiple
        #          times but it has a downside with flow control
        #          in that the flow logic won't know which stage to call
        #          next

        # Convert stages into consistent format
        self.process_stages()
        merge_from.process_stages()

        # Copy init stage if not already defined
        if not self.init_stage and merge_from.init_stage:
            self.init_stage = merge_from.init_stage

        # Delete auto_init stage because it is no longer valid
        self.auto_init_stage = None

        # For now all stages are preserved even if they have duplicate
        # identifiers
        # stage names in the merge_from must be tracked to update any
        # references to them within merge_from. Only merge_from may have
        my_stage_names = self.get_stage_names()
        merge_from_stage_names = merge_from.get_stage_names()
        for n in merge_from_stage_names:
            new_name = n
            if n in my_stage_names:
                # Begin rename process
                # Create a new name
                i = 2
                new_name = n + "_" + str(i)
                while new_name in my_stage_names or new_name in merge_from_stage_names:
                    i += 1
                    new_name = n + "_" + str(i)

                # Move stage to new name
                merge_from.stages[new_name] = merge_from.stages[n]
                merge_from.stages[new_name]["id"] = new_name
                merge_from.stages[new_name]["instance"] = i
                del merge_from.stages[n]

                # Find and replace all variable references to this stage
                var_re = r'\$\{stages\.(' + n + r')[-\w\s\. \[\]]+\}'
                util.find_replace_re(merge_from.get_dict(), var_re, new_name)
                merge_from.update_attributes()

                # Find and replace all flow controls referencing this stage
                for v in merge_from.stages.values():
                    for fc in self.flow_controls:
                        if fc in v and v[fc] == n:
                            v[fc] = new_name

            # Merge in new stage
            self.stages[new_name] = merge_from.stages[new_name]

        # Ckey
        # ckeys in current JCF take precedence
        x = deepcopy(merge_from.ckey)
        x.update(self.ckey)
        self.ckey = x

        # local
        x = deepcopy(merge_from.local)
        x.update(self.local)
        self.local = x

        # ckey_template
        # XXX templates in current JCF take precedence XXX
        # Note: Changed rules in bb3e9ae: now templates in merge JCF take
        # precedence
        x = deepcopy(merge_from.ckey_template)
        self.ckey_template.update(x)

        # job_timeout
        # TODO: take the higher of the two?  Add them?
        if merge_from.job_timeout:
            self.job_timeout = merge_from.job_timeout
        # job_group
        if merge_from.job_group:
            self.job_group = merge_from.job_group
        # configure
        if merge_from.configure:
            x = deepcopy(merge_from.configure)
            if self.configure:
                x.update(self.configure)
            self.configure = x

        # Re-process stages
        self.process_stages()

    def process_stages(self):
        '''
        Converts stages section into something usable by Agent. This involves
        changing stages from a ordered array to a dict with "order" keys,
        and "next_default" keys set appropriately.
        It also adds defaults for any required keys.

        The only check that made is for duplicate stage names. If encountered
        the dup stage is renamed by appending "_2" then "_3" etc.
        '''

        if not self.stages:
            # No stages to process
            return

        next_order_stages = dict()
        explicit_order_stages = list()
        implicit_order_stages = list()

        if isinstance(self.stages, list):
            # Make all names unique
            for stage_index in range(len(self.stages)):
                s = self.stages[stage_index]
                id = s.keys()
                if len(id) != 1:
                    raise ValueError("JCF " + str(self.path) +
                                     " stages section is corrupt, each" +
                                     " list element must contain exactly" +
                                     " one stage")

                id = base_id = id[0]
                v = s[id]

                # Create unique stage ID
                i = 2
                stage_names = [x.keys()[0] for x in self.stages]
                try:
                    # Remove my own ID from the list
                    stage_names.remove(id)
                except:
                    pass
                while id in stage_names:
                    id = base_id + "_" + str(i)
                    i += 1

                # Store new stage
                self.stages[stage_index] = { id: v }

                # Set self-reference
                v["id"] = id

                # Set initial instance if not present
                v.setdefault("instance", 1)

                # Find highest order
                if "order" in v:
                    try:
                        o = int(v["order"])
                    except:
                        raise ValueError("order field in stage {0} is not " +
                                         "an integer".format(base_id))
                    if o >= self.order:
                        self.order = o + 1

            # Separate stages according to these rules:
            # - Stages that have next* settings (highest sort precedence)
            # - Stages that have order field (lower sort precedence)
            # - Stages with neither (order is literal)
            for s in self.stages:
                if len(s.keys()) != 1:
                    raise ValueError("JCF " + str(self.path) +
                                     " stages section is corrupt, each list " +
                                     " element mustcontain exactly one stage")
                id = s.keys()[0]
                v = s.values()[0]

                if "next_pass" in v:
                    v["next_default"] = v["next_pass"]
                    if "order" in v:
                        del v["order"]
                    next_order_stages[id] = v
                elif "next_default" in v:
                    if "order" in v:
                        del v["order"]
                    next_order_stages[id] = v
                elif "order" in s:
                    if int(v["order"]) >= self.order:
                        # Find highest order number
                        self.order = int(v["order"]) + 1
                    explicit_order_stages.append(s)
                else:
                    implicit_order_stages.append(s)
        else:
            # Dict format
            # Find highest order number
            for s in self.stages.values():
                # Figure out highest order
                if "order" in s and \
                        int(s["order"]) >= self.order:
                    self.order = int(s["order"]) + 1

            # Separate stages according to these rules:
            # - Stages that have next* settings (highest sort precedence)
            # - Stages that have order field (lower sort precedence)
            # - Stages with neither (order is literal)
            for id, v in self.stages.items():
                s = {id: v}

                # Set self-reference
                v["id"] = id

                # Set initial instance if not present
                v.setdefault("instance", 1)

                if "next_default" in v:
                    if "order" in v:
                        del v["order"]
                    next_order_stages[id] = v
                elif "next_pass" in v:
                    v["next_default"] = v["next_pass"]
                    if "order" in v:
                        del v["order"]
                    next_order_stages[id] = v
                elif "order" in s:
                    if int(v["order"]) >= self.order:
                        # Find highest order number
                        self.order = int(v["order"]) + 1
                    explicit_order_stages.append(s)
                else:
                    implicit_order_stages.append(s)

        # COMMON PROCESSING for list format and dict format
        # Apply order to implicit stages
        for s in implicit_order_stages:
            v = s.values()[0]
            v["order"] = self.order
            self.order += 1
            explicit_order_stages.append(s)

        # Build new dict with correct order
        # - Stages that have next* settings (highest sort precedence)
        prev_stage = None
        prev_id = None
        auto_init_stage_set = False
        stage_paths = self._get_stage_path(path="next_default",
                                           stage_dict=next_order_stages)
        default_path_stages = stage_paths["next_default"]
        outside_path_stages = stage_paths["other"]

        for id, v in default_path_stages:
            # First id will be the first stage id
            if not auto_init_stage_set:
                self.auto_init_stage = id
                auto_init_stage_set = True

            if prev_stage:
                prev_stage["next_default"] = id
            prev_id = id
            prev_stage = next_order_stages[id]

        # - Stages that have order field (all the rest at this point)
        prev_stage = None
        prev_id = None
        for s in sorted(explicit_order_stages,
                        key=lambda(k): k.values()[0]["order"]):
            id = s.keys()[0]
            v = s.values()[0]

            # First id will be the first stage id
            if not auto_init_stage_set:
                self.auto_init_stage = id
                auto_init_stage_set = True

            next_order_stages[id] = v
            if prev_stage:
                prev_stage["next_default"] = id
            prev_stage = v
            prev_id = id
            del prev_stage["order"]

        # Stages that don't fit in the default path
        # TODO: this is just tacked on right now -- need to do more?
        for id, v in default_path_stages:
            next_order_stages[id] = v

        # Assign new stage structure to JCF
        self.stages = next_order_stages

        # Run through list again to gather any disable (skipped) stage names
        # Also add required keys if not present:
        # - target
        disabled_stages = list()
        for id, s in self.stages.items():
            if s.get("disable", False):
                disabled_stages.append(id)
            if "target" not in s:
                s["target"] = "${sut}"

        try:
            self.skip_stages(disabled_stages)
        except FlowError as e:
            raise ValueError("A disabled (skipped) stage may have " +
                             "created an infinite loop or you " +
                             "have skipped all stages, check JCF " +
                             "or enable a stage; " +
                             e.message)

    def process_system_vars(self):
        '''
        Interpolates Cirrus system variables--these are special variables
        set by the system automatically for convenience.
        '''
        # Get system variables
        # Primary SUT:
        # - If one sut defined, that becomes the default
        # - SUT labeled "sut"
        # - SUT with "default" set to some true value
        sys_sut = None
        sut_ids = self.suts.keys()
        mapping = {}
        if len(sut_ids) == 1:
            mapping[sut_ids[0]] = sut_ids[0]
        elif len(sut_ids) > 1:
            if "sut" in sut_ids:
                for sut_id in sut_ids:
                    mapping[sut_id] = sut_id
            else:
                # no sut found in suts section, try to find the one with default enable
                for sut_id in sut_ids:
                    if "default" in self.suts[sut_id] and self.suts[sut_id]["default"]:
                        mapping[sut_id] = sut_id
                        break
        else:
            mapping["sut"] = "(no suts defined)"

        # Convert SUT ID, if it is not an error, to ipaddress
        # TODO: develop a better mechanism for identifying SUTs to Agent and in
        #       GUI and JCF--ILO IP may not work for everything
        for k in mapping.keys():
            if k in self.suts:
                # sys_sut = self.suts[sys_sut].get("ipaddress", "(no IP address found)")
                # change "ipaddress" to "sys_ip"
                mapping[k] = self.suts[k].get("sys_ip", "(no IP address found)")

        # Cirrus Server (this system)
        # TODO: this will work for current activities which mainly validate that
        # the current system is the one we think it is for stage execution
        # (code in agent.py) but this needs to be refined to identify primary
        # IPs in multi-homed hosts.
        # See also: origination setting
        if "cirrus_ip" in self.info and self.info["cirrus_ip"]:
            cirrus_ip = self.info["cirrus_ip"]
        else:
            cirrus_ip = util.get_preferred_local_ip()

        # Now replace all variable instances
        mapping["cirrus"] = cirrus_ip

        for k, v in mapping.items():
            if not k.startswith("$"):
                k = "${" + k + "}"
            util.find_replace(self.info, k, v)
            util.find_replace(self.suts, k, v)
            util.find_replace(self.stages, k, v)

    def process_file_vars(self):
        '''
        This walks the entire JCF structure looking for variables and
        interpolates them base in local file scope
        '''
        if "_serial" in self.info:
            self.interpolate_variables(scope=self.info["_serial"])

    def process_stage_vars(self, stage=None):
        '''
        This walks the entire JCF structure looking for variables and
        interpolates them based on stage scope
        '''
        if stage:
            s = self.get_stage_by_name(stage)
            if s and "_serial" in s:
                self.interpolate_variables(scope=s["_serial"])
            else:
                self.interpolate_variables()
        else:
            self.interpolate_variables()

    def process_ckey_defaults(self):
        '''
        This scans ckey_template section and inserts any missing settings into
        the ckey section if they are not present.
        It also checks that expected lists appear in list format.
        '''
        if not self.ckey_template:
            return

        for serial in self.ckey_template:
            ckeyList = self.ckey_template[serial]
            for ct in ckeyList.keys():
                if ct not in self.ckey:
                    if "default" in ckeyList[ct]:
                        self.ckey[ct] = ckeyList[ct]["default"]
                    elif "hidden" in ckeyList[ct]:
                        raise ValueError("JCF " + str(self.path) +
                                         " hidden ckey_template setting " +
                                         "'{0}' must have a default field".format(ct))

                # Auto-create lists if expected
                def_type = ckeyList[ct].get("data_type", "")
                if (ct in self.ckey and
                        def_type.lower() == "list" and
                        not isinstance(self.ckey[ct], list)):
                    self.ckey[ct] = [self.ckey[ct]]

    def process_singletons(self):
        # Gather all singletons by their ID
        singletons = dict()
        singleton_chosen = dict()

        for stage_id, stage_data in self.stages.items():
            singleton_id = stage_data.get("singleton_group", False)
            if singleton_id:
                instance = stage_data["instance"]
                chosen = stage_data.get("singleton_choice", False)
                singleton_id = singleton_id.lower()
                if singleton_id not in singletons:
                    singletons[singleton_id] = dict()
                    singleton_chosen[singleton_id] = False

                singletons[singleton_id][stage_id] = stage_data
                if not singleton_chosen[singleton_id] and chosen:
                    singleton_chosen[singleton_id] = chosen

        # Now we have a dictionary of singleton IDs containing all stages
        # and we need to remove all but one of those stages. Choose either
        # the first or the last singleton depending on preference. The default
        # is the first.
        stages_to_remove = list()
        for singleton_id in singletons.keys():
            keep_stage = None
            chosen = singleton_chosen[singleton_id]
            if not chosen:
                chosen = "first"
            else:
                chosen = chosen.lower()

            # Find either the first or last instance
            for stage_id in singletons[singleton_id].keys():
                instance = int(singletons[singleton_id][stage_id]["instance"])
                if keep_stage is None:
                    keep_stage = stage_id
                elif chosen == "first" and \
                 instance < singletons[singleton_id][keep_stage]["instance"]:
                    keep_stage = stage_id
                elif chosen == "last" and \
                 instance > singletons[singleton_id][keep_stage]["instance"]:
                    keep_stage = stage_id

            # Remove all but chosen instance
            for stage_id in singletons[singleton_id].keys():
                if stage_id != keep_stage:
                    stages_to_remove.append(stage_id)

        # Remove duplicate singletons
        try:
            self.remove_stages(stages_to_remove)
        except FlowError as e:
            raise ValueError("Infinite loop detected while removing " +
                             "duplicate stages: " +
                             ",".join(stages_to_remove) +
                             "; this JCF cannot be processed until that " +
                             "is resolved; " +
                             e.message)

    def import_system_config(self):
        pass

    def copy(self):
        return deepcopy(self)

    def get_dict(self):
        '''
        Refreshes the raw member which is the raw JSON content (in a Python
        dict) and returns the data.
        '''
        self._raw = dict()
        for s in self.section_members:
            data = getattr(self, s)
            if data:
                self._raw[s] = data

        return self._raw

    def update_attributes(self):
        # Reload data for all members
        self.info = self._raw.get("info", dict())
        self.init_stage = self._raw.get("init_stage", None)
        self.tags = self._raw.get("tags", [])
        self.include = self._raw.get("include", [])
        self.included = self._raw.get("included", [])
        self.local = self._raw.get("local", dict())
        self.tags = self._raw.get("tags", [])
        self.suts = self._raw.get("suts", dict())
        self.stages = self._raw.get("stages", [])
        self.ckey = self._raw.get("ckey", dict())
        self.local_ckey = self._raw.get("local_ckey", dict())
        self.ckey_template = self._raw.get("ckey_template", dict())
        self.module_info = self._raw.get("module_info", dict())
        self.dynamic = self._raw.get("dynamic", dict())
        self.origination = self._raw.get("origination", None)
        self.job_timeout = self._raw.get("job_timeout", None)
        self.configure = self._raw.get("configure", None)
        self.optionTextMap = self._raw.get("optionTextMap", None)
        self.id = self._raw.get("id", None)
        self.job_group = self._raw.get("job_group", [])

    def get_raw(self):
        # Function renamed to get_dict()
        return self.get_dict()

    def get_json(self):
        # Returns stringified JSON syntax of this object
        return json.dumps(self.get_dict())

    def write(self, filename=None):
        if filename is None:
            filename = self.path

        # Write JSON
        util.write_json(filename, self.get_raw())

    def get_stage_names(self):
        return self.stages.keys()

    def get_stage_by_name(self, stage_name):
        return self.stages.get(stage_name, None)

    def get_substage_by_name(self, stage_name, substage_name="action"):
        s = self.stages.get(stage_name, None)
        if s and substage_name in s:
            return s[substage_name]
        else:
            return None

    def get_stage_by_serial(self, serial):
        for s, v in self.stages.items():
            if "_serial" in v and v["_serial"] == serial:
                return s, v
        return None, None

    def get_substage_by_serial(self, serial, substage_name="action"):
        for id, s, ss in self.get_substages(substage=substage_name):
            if "_serial" in ss and ss["_serial"] == serial:
                return id, s, ss
        return None, None, None

    def get_substage_module_name(self, stage_name, substage_name="action"):
        s = self.stages.get(stage_name, None)
        if s and substage_name in s:
            return s[substage_name].get("cirrus_module", None)
        else:
            return None

    def _get_stage_path(self, path="next_default", init_stage=None, stage_dict=None):
        '''
        Returns a dict containing one or more lists of tuples representing the
        stage names and their data. Each set of tuples represents various
        categories of stages:
        - <path>: stages that follow a sequence and will be executed within
          a job assuming the "path" is used as the path. The default path
          is "next_default", or the default stage path. Therefore the key in
          the dict is also called "next_default".
        - other: stages that fall outside of the path or "path"

        Notes:
        - Calls process_stages if it has not already been done
        - ordered + standalone will represent all stages within a job

        path = the path by which to establish the order. Any "next*"
        setting will work assuming it is present. Defaults to the default
        path (next_default).

        init_stage = the name of the stage to start the path from. The default
        is the first stage of the JCF as determined by process_stages()
        (the value of this is stored in auto_init_stage member).
        '''
        ordered_stages = list()
        unordered_stages = list()
        visited_stages = dict()

        if stage_dict is None:
            if not isinstance(self.stages, dict):
                self.process_stages()
            stage_dict = self.stages

        if not stage_dict:
            return {
                path: [],
                "other": []
            }

        # Determine initial stage in this order of precedence:
        # - override passed into this function
        # - user-supplied init_stage
        # - auto_init_stage as calculated by process_stages
        s = init_stage or self.init_stage or self.auto_init_stage
        if not s:
            # No init_stage so figure it out
            all_stages = stage_dict.keys()

            for id in copy(all_stages):
                # The first stage is detected by looking for the one stage
                # that won't have anything else pointing to it. This approach
                # will work with most cases. Anything else is a configuration
                # error.
                if "order" in stage_dict[id] and stage_dict[id]["order"] is 1:
                    all_stages = [id]
                    break
                if "next_pass" in stage_dict[id]:
                    if stage_dict[id]["next_pass"] in all_stages:
                        all_stages.remove(stage_dict[id]["next_pass"])
                elif "next_default" in stage_dict[id]:
                    if stage_dict[id]["next_default"] in all_stages:
                        all_stages.remove(stage_dict[id]["next_default"])

            if len(all_stages) == 1:
                # Found the most likely initial stage
                s = all_stages[0]
            else:
                err = "Cannot figure out initial stage from subset" + \
                      " " + str(sorted(stage_dict.keys())) + "."
                if len(all_stages) > 1:
                    err += " Ambiguous candidates: " + str(sorted(all_stages))
                else:
                    err += " No candidates. Possible illegal stage loop."
                err += " You could use 'init_stage' setting to resolve this."
                raise ValueError(err)

        while(1):
            if s in visited_stages:
                # If stage is encountered twice we are in a loop, quit now
                s = "_quit"
            elif s in stage_dict:
                # Stage is in JCF, add to list and use 'path' path to find
                # the next
                ordered_stages.append((s, stage_dict[s]))
                visited_stages[s] = True
                next_stage = stage_dict[s].get(path, "_next")
                if not next_stage or next_stage == "_next":
                    next_stage = stage_dict[s].get("next_default", "_quit")
                    if not next_stage:
                        next_stage = "_quit"
                s = next_stage
            else:
                s = "_quit"

            if s == "_quit":
                break

        # Calculate standalone stages
        for id in sorted(list(set(stage_dict.keys()) - set(visited_stages.keys()))):
            unordered_stages.append((id, self.stages[id]))

        return {
            path: ordered_stages,
            "other": unordered_stages
        }

    def get_ordered_stages(self, path="next_default", init_stage=None, stage_dict=None):
        '''
        Returns a list of tuples representing the stage names and data in the
        order they are executed.

        Notes:
        - Calls process_stages if it has not already been done
        - It is possible that some stages will not be returned if they are
          not part of the path. e.g. if a stage is meant to handle a failure
          and is only reached via "next_fail".
          (You can use get_standalone_stages() to retrieve those.)

        path = the path by which to establish the order. Any "next*"
        setting will work assuming it is present. Defaults to the default
        path (next_default).

        init_stage = the name of the stage to start the path from. The default
        is the first stage of the JCF as determined by process_stages()
        (the value of this is stored in auto_init_stage member).
        '''
        return self._get_stage_path(path, init_stage, stage_dict)[path]

    def get_unordered_stages(self, path="next_default", init_stage=None, stage_dict=None):
        '''
        Returns a list of tuples representing the stage names and data that
        fall outside of the perceived order. Essentially these are stages
        that will not ever be called under the given path.

        Notes:
        - Calls process_stages if it has not already been done

        path = the path by which to establish the order. Any "next*"
        setting will work assuming it is present. Defaults to the default
        path (next_default).

        init_stage = the name of the stage to start the path from. The default
        is the first stage of the JCF as determined by process_stages()
        (the value of this is stored in auto_init_stage member).
        '''
        return self._get_stage_path(path, init_stage, stage_dict)["other"]

    def get_first_stage(self, stage_dict=None):
        '''
        Returns the first tuple that comes out of get_ordered_stages()
        which is (stage name, stage data)
        '''
        stages = self.get_ordered_stages(stage_dict=stage_dict)
        if not stages:
            return None
        return stages[0]

    def get_last_stage(self, stage_dict=None):
        '''
        Returns the last tuple that comes out of get_ordered_stages()
        which is (stage name, stage data)
        '''
        stages = self.get_ordered_stages(stage_dict=stage_dict)
        if not stages:
            return None
        return stages[-1]

    def get_job_group(self):
        return [item for item in self.job_group if item.get('placeholder') is None]

    def get_sut_names(self):
        return self.suts.keys()

    def get_sut_by_name(self, name):
        return self.suts.get(name, None)

    def get_substages(self, substage="all"):
        '''
        Generator for finding all substages (action, validate, report) within
        this JCF. The generator returns a tuple:
        - stage ID (str)
        - substage ID(str) - one of: action, validate, report
        - substage data (dict) - e.g. self.stages[id]["action"] where id is the
          first tuple element
        '''
        for id, s in self.stages.items():
            if substage == "all" or substage == "action":
                if "action" in s:
                    yield (id, "action", s["action"])
            if substage == "all" or substage == "validate":
                if "validate" in s:
                    yield (id, "validate", s["validate"])
            if substage == "all" or substage == "report":
                if "report" in s:
                    yield (id, "report", s["report"])

    def get_ckey(self, key=None, scope=None, default=None):
        r = self.get_local_ckey(key, scope, default)
        g = self.get_global_ckey(key, default)

        if key:
            return r or g
        else:
            if r is None:
                return g
            elif g:
                g = deepcopy(g)
                g.update(r)
                return g
            else:
                return r

    def get_global_ckey(self, key=None, default=None):
        if key is not None:
            if key in self.ckey:
                return self.ckey[key]
            else:
                return default
        else:
            return self.ckey

    def get_local_ckey(self, key=None, scope=None, default=None):
        if not scope:
            scope = self._serial

        if scope == "*":
            # Search everything
            if not key:
                raise ValueError("Search of all scopes (*) requires a key")
            scope = self.get_scope(key)
            if not scope:
                return default
        elif scope not in self.local:
            return default

        if key is not None:
            if key in self.local[scope]:
                return self.local[scope][key]
            else:
                return default
        else:
            return self.local[scope]

    def get_scope(self, key):
        '''
        Returns the first scope that contains a given key
        '''
        for s in self.local.keys():
            if key and key in self.local[s]:
                return s
        return None

    def _interpolate_string(self, string, scope=None, filter=""):
        '''
        This is the atomic function at the heart of all variable interpolation.
        It is the code that does the actual substitution.

        string = the string to interpolate

        scope  = the specific scope to look at (serial number of the object).
                 If set to None (default) then use global scope.
                 Special scopes:
                   "*" means search every scope that is available and returns
                   the first occurrence of a variable. If multiple occurrences
                   exist then one returned is unpredictable under "*" scope.

        filter = Limits interpolation to variables that start with filter.
                 For example if "suts" is the filter then only ${suts...}
                 variables will be interpolated.
        '''
        var_re = r'(\$\{(' + filter + r'[- \w\s\.\:\[\]]+)\})'
        obj_data = self.get_dict()

        # Outer while loop will ensure we get all nested variables like
        # string = "The ${var1} jumped over the fence"
        #     ${var1} = "${var2}"
        #     ${var2} = "dog"
        loops = 0
        while True:
            matches = re.findall(var_re, string)
            if not matches:
                break
            loops += 1

            if loops >= 100:
                raise ValueError("JCF " + str(self.path)
                                 + "Infinite loop detected on string '"
                                 + string)

            dups = dict()
            for replaceme, key in matches:
                # Skip duplicate matches as they have already been processed
                if key in dups:
                    continue
                else:
                    dups[key] = True

                # Change path1.path2.key into ['path1']['path2']['key']
                key_parts = key.split(".")
                key_string = str()

                # Handle local variables (ckeys only) by redirecting
                # interpolation to local section if the given ckey is
                # found there
                if scope and len(key_parts) > 1 and key_parts[0] == "ckey":
                    k = key_parts[1]
                    if scope == "*":
                        scope = self.get_scope(k)
                    if self.get_local_ckey(k, scope) != None:
                        key_parts = list(key_parts)
                        key_parts[0] = "local"
                        key_parts.insert(1, scope)

                # Look at each key part and identify any array refs
                for kp in key_parts:
                    m = re.search(r'(.+?)(\[\d+\])$', kp)
                    if m:
                        # array form: ['key'][#]
                        key_string += "['" + m.group(1) + "']" + m.group(2)
                    else:
                        # non-array form: ['key']
                        key_string += "['" + kp + "']"

                # See if key exists in this object
                try:
                    value = eval("obj_data" + key_string)

                    # Return String or Structure?
                    #
                    # If variable is by itself like "${var}" as opposed to
                    # something like "My name is ${var}" then we allow this
                    # to morph into a structure reference.
                    # First we detect this situation by comparing the
                    # seeing if the variable string is the entire "replaceme."
                    # In this case we need to check if the value we looked
                    # up is a string or a structure. If it is not a str/unicode
                    # then it must be a structure and so we will return
                    # the structure instead.
                    # However, if it turns out to be a string we treat it
                    # normally and keep processing it for multiple
                    # interpolation.
                    if (replaceme == string and
                            not isinstance(value, str) and
                            not isinstance(value, unicode)):
                        return value

                except:
                    # There is no corresponding value for this key so flag the
                    # object as not fully interpolated, change the key so it
                    # appears empty which will prevent it from being processed
                    # again.
                    self.interpolation_errors.append(replaceme)
                    value = replaceme.replace("${", "${}{")

                # Replace all instances
                string = string.replace(replaceme, value)

        # Revert any missing values to their original state
        string = string.replace("${}", "$")
        return string

    def get_stage_ckey(self, stage_id, ckey=None, default=None):
        s = self.get_stage_by_name(stage_id)
        if not s:
            return None

        return self.get_local_ckey(ckey, s.get("_serial", None), default)

    def _interpolate_structure(self, structure, scope=None, filter=""):
        '''
        Recursive function to descend JCF structure and interpolate every
        string

        structure = the structure to interpolate
        scope     = the specific scope to look at (serial number of the object).
                    If set to None (default) then use global scope.
                    There is a special filter called "*" which means search
                    every scope that is available and returns the first
                    occurrence of a variable. If multiple occurrences exist
                    then one returned is unpredictable under "*" scope.
        filter    = Limits interpolation to variables that start with filter.
                    For example if "suts" is the filter then only ${suts...}
                    variables will be interpolated.

        '''
        if isinstance(structure, str) or isinstance(structure, unicode):
            return self._interpolate_string(structure, scope, filter)
        elif isinstance(structure, dict):
            if "_serial" in structure:
                scope = structure["_serial"]
            for k in structure.keys():
                orig = structure[k]
                structure[k] = self._interpolate_structure(
                    structure[k], scope, filter)
        elif isinstance(structure, list):
            for i in range(len(structure)):
                structure[i] = self._interpolate_structure(
                    structure[i], scope, filter)

        # This will return as-is data types that cannot be interpolated
        # (numbers, booleans, etc.) or return the recursively modified
        # structure.
        return structure

    def interpolate_variables(self, section=None, scope=None, filter=""):
        '''
        Scan entire object for strings that look like variables and replace
        them with actual values.

        The internal variable format is specific to Cirrus, here is an
        example:
            ${stages.My Stage.action.cirrus_module}
        This corresponds to the dict representation of a JCF:
            self.get_dict()['stages']['My Stage']['action'][cirrus_module']

        Nested variables (an interpolated variable that contains another
        variable to interpolate) are permitted as long as they don't create
        a loop.

        section = the section to interpolate (e.g. suts or stages). If
                  set to None (default) then all sections that support
                  variables are processed
        scope   = the specific scope to look at (serial number of the object).
                  If set to None (default) then use global scope.
                  There is a special filter called "*" which means search
                  every scope that is available and returns the first
                  occurrence of a variable. If multiple occurrences exist
                  then one returned is unpredictable under "*" scope.
        filter  = Limits interpolation to variables that start with filter.
                  For example if "suts" is the filter then only ${suts...}
                  variables will be interpolated.

        Sections that can contain variables:
            suts
            stages
            ckey
            local_ckey
            info
        '''

        # Clear interpolation errors, if there is something missing it will
        # be recorded.
        self.interpolation_errors = list()

        if section is not None and not isinstance(section, list):
            section = [section]

        # Interpolate all supported sections
        if not section or "suts" in section:
            self.suts = self._interpolate_structure(self.suts, scope, filter)
            # Convert list to string for SUT name and ID
            for sut_name in self.suts:
                sut_data = self.suts[sut_name]
                for f in "id", "name":
                    if f in sut_data and isinstance(sut_data[f], list) and sut_data[f]:
                        sut_data[f] = sut_data[f][0]
        if not section or "stages" in section:
            self.stages = self._interpolate_structure(self.stages, scope, filter)
        if not section or "ckey" in section:
            self.ckey = self._interpolate_structure(self.ckey, scope, filter)
        if not section or "info" in section:
            self.info = self._interpolate_structure(self.info, scope, filter)

    def resolve_sut(self, id):
        # See if ID is literal
        if id in self.suts:
            return self.suts[id]

        # ID is an IP address
        for s in self.suts.values():
            if "sys_ip" in s and s["sys_ip"] == id:
                return s

        return None

    def get_sut(self, id):
        # See if ID is literal
        if id in self.suts:
            return id, self.suts[id]

        # ID is an IP address
        for sut_real_id, s in self.suts.items():
            if "sys_ip" in s and s["sys_ip"] == id:
                return sut_real_id, s

        # ID is an internal ID
        for sut_real_id, s in self.suts.items():
            if "id" in s and int(s["id"]) == int(id):
                return sut_real_id, s

        return None, None

    def get_configuration(self, setting, stage=None):
        # get setting from configure
        if stage:
            if stage in self.stages.keys():
                if 'configure' in self.stages[stage].keys() and \
                    setting in self.stages[stage]['configure'].keys():
                    return self.stages[stage]['configure'][setting]
            else:
                raise ValueError("Invalid stage '" + stage)
        if self.configure:
            if setting in self.configure.keys():
                return self.configure[setting]
        return None

    def skip_stages(self, stage_list):
        '''
        Re-route around a set of stages by changing all references to those
        stages to point to the next stage in the flow.

        stage_list is a list of stage IDs to skip

        Exceptions:
            FlowError if an infinite loop is detected
        '''
        if not stage_list:
            return

        reroutes = True
        depth = 0
        while reroutes:
            reroutes = False
            depth += 1

            # Possible looping stage, give up
            if self.max_depth != -1 and depth > self.max_depth:
                raise FlowError("Infinite loop detected in flow; depth=" +
                                str(depth - 1))

            # Route init stage
            if self.init_stage in stage_list:
                d_stage = self.stages[self.init_stage]
                if "next_pass" in d_stage:
                    new_fc = d_stage["next_pass"]
                else:
                    new_fc = d_stage["next_default"]
                self.init_stage = new_fc
                reroutes = True

            # Route any other stages so they point to the next stage
            # If the next stage is also disabled it will be rerouted
            # again on the next loop
            for id, s in self.stages.items():
                for fc in self.flow_controls:
                    if s.get(fc, None) in stage_list:
                        d_stage = self.stages[s[fc]]
                        if "next_pass" in d_stage:
                            new_fc = d_stage["next_pass"]
                        else:
                            new_fc = d_stage.get(fc, d_stage["next_default"])
                        s[fc] = new_fc
                        reroutes = True

    def remove_stages(self, stage_list):
        '''
        Similar to skip_stage except the stage is not only routed around but
        removed entirely

        stage_list is a list of stage IDs to remove

        Exceptions:
            FlowError if an infinite loop is detected (raised by skip_stages)
        '''
        self.skip_stages(stage_list)
        for s in stage_list:
            if s in self.stages:
                del self.stages[s]

# Utility class functions for use with modules

# TODO: Rename Module to something else--the term is overloaded and not accurate in this case
class Module(JCF):

    def __init__(self, leader_file):
        leader_file = abspath(leader_file)
        if not isfile(leader_file):
            raise ValueError("Leader file " + leader_file + " not found")

        JCF.__init__(self, leader_file)
        self.process_stages()
        self.process_includes()
        self.process_stages()
        self.process_file_vars()

        # Set various areas based on the location of leader_file
        self.module_info["substage_working_area"] = dirname(leader_file)
        self.module_info["stage_working_area"] = dirname(dirname(leader_file))
        self.module_info["job_working_area"] = dirname(dirname(dirname(dirname(leader_file))))
        self.module_info["job_status_file"] = join(self.module_info["job_working_area"], "status.json")
        module_name = self.get_substage_module_name(self.module_info["stage_id"], self.module_info["substage_id"])
        if module_name:
            self.module_info["module_working_area"] = join(self.module_info["job_working_area"], "module", module_name)
        else:
            self.module_info["module_working_area"] = None

        # Built-in job status access
        self.status = Status(
            self.module_info["job_status_file"],
            autoupdate=True)
        self.live_status = LiveStatus(
            self.module_info["job_working_area"],
            self.module_info["job_status_file"],
            autoupdate=True)
        self.live_jcf = LiveJCF(self.module_info["job_working_area"])

    def interpolate_string(self, string, scope=None, filter=""):
        '''
        A public version of _interpolate_string(). It is slightly different
        in that the scope default is that of the active stage, and it will
        also interpolate system variables.

        This is the atomic function at the heart of all variable interpolation.
        This public version is meant to be used to extend Cirrus variables
        to external files or data. (e.g. read file in as string, pass it into
        this function, then write it out again.)

        string = the string to interpolate

        scope  = the specific scope to look at (serial number of the object).
                 If set to None (default) then use current stage scope.
                 Special scopes:
                   "*" means search every scope that is available and returns
                   the first occurrence of a variable. If multiple occurrences
                   exist then one returned is unpredictable under "*" scope.

                   "^" force global scope only

        filter = Limits interpolation to variables that start with filter.
                 For example if "suts" is the filter then only ${suts...}
                 variables will be interpolated.
        '''

        s = str()
        if scope == "^":
            s = self._interpolate_string(string, None, filter)
        elif scope is None:
            stage = self.get_stage_id()
            s = self.get_stage_by_name(stage)
            if s and "_serial" in s:
                s = self._interpolate_string(string, s["_serial"], filter)

        # Covers all other cases
        if not s:
            s = self._interpolate_string(string, scope, filter)

        # Write out system variable for job ID
        for sv in (
            "substage_working_area",
            "stage_working_area",
            "job_working_area"
        ):
            s = s.replace("${" + sv + "}", self.module_info[sv])

        s = s.replace("${cirrus_stage_id}", self.module_info["stage_id"])
        s = s.replace("${cirrus_substage_id}", self.module_info["substage_id"])
        s = s.replace("${cirrus_stage_sut}", self.get_stage_sut())
        s = s.replace("${cirrus_job_id}", self.info.get("cirrus_job_id", "UNKNOWN"))

        return s

    def get_module_settings(self):
        '''
        Utility to load a stage-specific JCF and return pertinent settings back to
        a module.

        leader_file = full path to the leader JCF that contains stage-specific data
        '''
        stage_id = self.module_info["stage_id"]
        substage_id = self.module_info["substage_id"]
        if stage_id in self.stages and substage_id in self.stages[stage_id]:
            return self.stages[stage_id][substage_id]
        else:
            raise LookupError("{0}/{1}".format(stage_id, substage_id) +
                              " not in stage structure, JCF corrupted!")

    def get_job_working_area(self):
        return self.module_info["job_working_area"]

    def get_stage_working_area(self):
        return self.module_info["stage_working_area"]

    def get_substage_working_area(self):
        return self.module_info["substage_working_area"]

    def get_module_working_area(self):
        return self.module_info["module_working_area"]

    def get_job_status_file(self):
        return self.module_info["job_status_file"]

    def get_stage_id(self):
        return self.module_info["stage_id"]

    def get_substage_id(self):
        return self.module_info["substage_id"]

    def get_stage_data(self):
        return self.get_stage_by_name(self.get_stage_id())

    def get_stage_sut(self):
        data = self.get_stage_data()
        if "stage_sut" in data:
            return data["stage_sut"]
        else:
            return "sut"

    def get_substage_data(self):
        return self.get_substage_by_name(self.get_stage_id(), self.get_substage_id())

    def get_stage_target(self):
        return self.get_stage_data()["target"]

    def get_stage_sut_data(self):
        sut_id, sut_data = self.get_sut(self.get_stage_sut())
        return sut_data

    def get_ckey(self, key=None, scope=None, default=None):
        if scope is None:
            scope = self.get_stage_data().get("_serial", None)
        return super(Module, self).get_ckey(key, scope, default)

    def get_local_ckey(self, key=None, scope=None, default=None):
        if scope is None:
            scope = self.get_stage_data().get("_serial", None)
        return super(Module, self).get_local_ckey(key, scope, default)


class Status(object):

    def __init__(self, status_file, autoupdate=True):
        self.status_file = status_file
        self.status = None
        self.autoupdate = autoupdate

    def update(self):
        self.status = util.read_json(self.status_file)

    def write(self):
        util.write_json(self.status_file, self.status)

    def _get_job_data(self, key):
        if self.autoupdate:
            self.update()
        if self.status and key in self.status:
            return self.status[key]
        else:
            return None

    def _get_stage_data(self, name, key):
        if self.autoupdate:
            self.update()
        if self.status and "stages" in self.status and name in self.status["stages"]:
            return self.status["stages"][name].get(key, None)
        return None

    def _set_stage_data(self, name, key, value):
        if self.autoupdate:
            self.update()

        if not self.status:
            self.status = dict()

        if "stages" not in self.status:
            self.status["stages"] = dict()

        if name in self.status["stages"]:
            self.status["stages"][name][key] = value
        else:
            self.status["stages"][name] = {key:value}

        self.write()

    def stage_ran(self, name):
        s = self.get_stage_status(name)
        if s:
            return True
        else:
            return False

    def get_stage_status(self, name):
        return self._get_stage_data(name, "status")

    def get_stage_code(self, name):
        return self._get_stage_data(name, "code")

    def get_stage_signal(self, name):
        return self._get_stage_data(name, "signal")

    def get_stage_message(self, name):
        return self._get_stage_data(name, "message")

    def set_stage_message(self, name, message):
        self._set_stage_data(name, "message", message)

    def append_stage_message(self, name, message):
        try:
            current_message = self._get_stage_data(name, "message")
        except:
            current_message = ""
        if current_message:
            self._set_stage_data(name, "message", current_message + "; " + message)
        else:
            self._set_stage_data(name, "message", message)

    def get_stage_time_start(self, name):
        return self._get_stage_data(name, "time_start")

    def get_stage_time_end(self, name):
        return self._get_stage_data(name, "time_end")

    def get_stage_duration(self, name):
        end = self.get_stage_time_end(name)
        start = self.get_stage_time_start(name)
        if not start or not end:
            return None
        return end - start

    def get_job_status(self):
        return self._get_job_data("status")

    def get_job_code(self):
        return self._get_job_data("code")

    def get_job_signal(self):
        return self._get_job_data("signal")

    def get_job_message(self):
        return self._get_job_data("message")

    def get_job_time_start(self):
        return self._get_job_data("time_start")

    def get_job_time_end(self):
        return self._get_job_data("time_end")

    def get_job_duration(self):
        end = self.get_job_time_end()
        start = self.get_job_time_start()
        if not start or not end:
            return None
        return end - start


class LiveStatus(Status):
    '''
    Same as Status but uses the Agent for certain I/O operations
    '''
    def __init__(self, agent_working_dir, status_file, autoupdate=True):
        super(LiveStatus, self).__init__(status_file, autoupdate)
        self.status_queue = util.EventQueue("status", queue_root_dir=agent_working_dir, queue_type="sender")

    def _wait_for_receiver(self, event_id):
        while not self.status_queue.wait_all_events_processed(event_id):
            time.sleep(3)

    def _set_stage_data(self, name, key, value):
        if self.autoupdate:
            self.update()

        d = {
            "stages": {
                name: {
                    key: value
                }
            }

        }

        msg = util.Event(d)
        msg.type = "update"
        self.status_queue.put(msg)
        self.status_queue.send_events()
        self._wait_for_receiver(msg.event_id)


class LiveJCF():

    def __init__(self, agent_working_dir):
        self.jcf_queue = util.EventQueue("jcf", queue_root_dir=agent_working_dir, queue_type="sender")

    def _wait_for_receiver(self, event_id):
        while not self.jcf_queue.wait_all_events_processed(event_id):
            time.sleep(3)

    def update(self, data):
        msg = util.Event(data)
        msg.type = "update"
        self.jcf_queue.put(msg)
        self.jcf_queue.send_events()
        self._wait_for_receiver(msg.event_id)
