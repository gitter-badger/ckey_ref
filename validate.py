from __future__ import unicode_literals, print_function, absolute_import

from os.path import abspath, join as pjoin, dirname as pdir
import json
import re

#from jsonschema import Draft4Validator


SCHEMA_FOLDER_NAME = 'jcf_schema'
MAIN_SCHEMA = 'main.json'


class VersionNumberError(Exception): pass


#def load_schema(rule_version):
#    script_folder = pdir(__file__)
#    schema_folder = pjoin(script_folder, SCHEMA_FOLDER_NAME)
#    schema_path = abspath(pjoin(schema_folder, rule_version, MAIN_SCHEMA))
#
#    try:
#        schema = json.load(open(schema_path))
#    except Exception as e:
#        raise SchemaError(*e.args)
#
#    return schema


def get_schema_path(rule_version):
    script_folder = pdir(__file__)
    schema_folder = pjoin(script_folder, SCHEMA_FOLDER_NAME)
    schema_path = abspath(pjoin(schema_folder, rule_version, MAIN_SCHEMA))
    return schema_path


#def main(abs_file_path, rule_version=None):
#    script_folder = os.path.dirname(__file__)
#    schema_folder_name = 'jcf_schema'
#    schema_folder = os.path.join(script_folder, schema_folder_name)
#    rule_version = convert_rule_version(rule_version) if rule_version else\
#        get_latest_schema_version_from(schema_folder)
#    main_schema = 'main.json'
#    schema_path = os.path.abspath(os.path.join(schema_folder, rule_version,
#                                               main_schema))
#    with open(schema_path) as s:
#        schema = json.load(s)
#
#    with open(abs_file_path) as t:
#        target = json.load(t)
#
#    sematic_check(target, schema)
#    ckey_reference_check()


def sematic_check(target, schema):
    '''
    '''
    # mimic behavior of `validate` to further classify errors
    for error in Draft4Validator(schema).iter_errors(target):
        raise error
    # else:
    #     pass


def ckey_reference_check():
    '''
    '''
    pass


def _interpolate_string(string, scope=None, filter=""):
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
    # def get_dict():
    #     '''
    #     Refreshes the raw member which is the raw JSON content (in a Python
    #     dict) and returns the data.
    #     '''
    #     _raw = dict()
    #     for s in section_members:
    #         data = getattr(self, s)
    #         if data:
    #             _raw[s] = data

    #     return _raw

    #     orignial data?
    obj_data = get_dict()

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

        # TODO
        path = 'file_path'
        if loops >= 100:
            raise ValueError("JCF " + str(path)
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
                    # def get_scope(self, key):
                    #     '''
                    #     Returns the first scope that contains a given key
                    #     '''
                    #     for s in self.local.keys():
                    #         if key and key in self.local[s]:
                    #             return s
                    #     return None
                    scope = self.get_scope(k)
                if self.get_local_ckey(k, scope) is not None:
                    # def get_local_ckey(self, key=None, scope=None, default=None):
                    #     if not scope:
                    #         scope = self._serial

                    #     if scope == "*":
                    #         # Search everything
                    #         if not key:
                    #             raise ValueError("Search of all scopes (*) requires a key")
                    #         scope = self.get_scope(key)
                    #         if not scope:
                    #             return default
                    #     elif scope not in self.local:
                    #         return default

                    #     if key is not None:
                    #         if key in self.local[scope]:
                    #             return self.local[scope][key]
                    #         else:
                    #             return default
                    #     else:
                    #         return self.local[scope]
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
                # value = eval("obj_data" + key_string)
                value = obj_data[key_string]

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
                        not isinstance(value, (str, unicode))):
                    return value
            except:
                # There is no corresponding value for this key so flag the
                # object as not fully interpolated, change the key so it
                # appears empty which will prevent it from being processed
                # again.
                # interpolation_errors.append(replaceme)
                value = replaceme.replace("${", "${}{")

            # Replace all instances
            string = string.replace(replaceme, value)

    # Revert any missing values to their original state
    string = string.replace("${}", "$")
    return string


def unify_rule_version(version):
    r"""
    unify version number from "a.b.c" to "a_b_c"

    >>> print(unify_rule_version('1.12.4'))
    1_12_4
    >>> print(unify_rule_version('2_0_5'))
    2_0_5
    >>> print(unify_rule_version('a____A'))
    Traceback (most recent call last):
        ...
    VersionNumberError: Not a good form. Expected form: "2_0_5", or "1.12.4"
    """
    if re.match(r'^\d+_\d+_\d+$', version):
        return version
    elif re.match(r'^\d+\.\d+\.\d+$', version):
        return version.replace('.', '_')
    else:
        raise VersionNumberError('Not a good form. Expected form: "2_0_5", or "1.12.4"')


#def convert_rule_version(rule_string):
#    '''
#    >>> expected_version_string = '1.12.4'
#    >>> convert_rule_version(expected_version_string)
#    u'1_12_4'
#    >>> converted_version_string = '2_0_5'
#    >>> convert_rule_version(converted_version_string)
#    u'2_0_5'
#    >>> bomb = '2.0'
#    >>> try:
#    ...     convert_rule_version(bomb)
#    ... except Exception as e:
#    ...     print(e)
#    Not a good form. Expected form: "2_0_5", or "1.12.4"
#    '''
#    if '_' in rule_string and '.' not in rule_string and\
#            len(rule_string.split('_')) == 3:
#        return rule_string
#    elif '.' in rule_string and '_' not in rule_string and\
#            len(rule_string.split('.')) == 3:
#        return rule_string.replace('.', '_')
#    else:
#        raise Exception('Not a good form. Expected form: "2_0_5", or "1.12.4"')



def get_latest_rule_version(names):
    r"""
    filter names according to version format , and get the latest one

    the version format is defined as "\d+_\d+_\d+"

    >>> names = ['2_9_1', '3_1_3', '1.3.4', '2_9', 'h_l_o' , 'ray_zhu']
    >>> print(get_latest_rule_version(names))
    3_1_3
    """
    patt = re.compile(r'^(\d+)_(\d+)_(\d+)$')
    match_objs = ((patt.match(name), name) for name in names)
    filtered = ((m, name) for m, name in match_objs if m)
    latest_pair = max(filtered, key=lambda pair: tuple(map(int, pair[0].groups())) )
    latest = latest_pair[1]
    return latest



#def get_latest_schema_version_from(folder):
#    '''
#    example `os.listdir`
#    ['2_9_1', '3_1_3', '1.3.4', '2_9', 'h_l_o']
#    after `filter`
#    ['2_9_1', '3_1_3', 'h_l_o']
#    after `map`
#    [('2', '9', '1'), ('3', '1', '3'), ('h', 'l', 'o')]
#    after `filter`
#    [('2', '9', '1'), ('3', '1', '3')]
#    after `max`
#    ('3', '1', '3')
#    after `join`
#    '3_1_3'
#    '''
#    return '_'.join(max(filter(lambda e: all(el.isdigit() for el in e),
#                               map(lambda x: tuple(x.split('_')),
#                                   filter(lambda f: len(f.split('_')) == 3,
#                                          os.listdir(folder))))))

def is_valid_jcf(path, version=None):
    """
    :param path: an absolute file path
    :param version: the rule version
    """
    if version is None:
        version = get_latest_schema_version(schema_folder)
    else:
        version = unify_rule_version(version)

    schema = json.load(open(get_schema_path(version)))
    target = json.load(open(path))

    return pass_sematic_check(target, schema) and pass_ckey_reference_check(target)


