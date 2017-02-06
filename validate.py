from __future__ import print_function, absolute_import

from os.path import abspath, join as pjoin, dirname as pdir
import os
import json
import re

#from jsonschema import Draft4Validator


SCHEMA_FOLDER_NAME = 'jcf_schema'
MAIN_SCHEMA = 'main.json'
SCHEMA_FOLDER = pjoin(pdir(__file__), SCHEMA_FOLDER_NAME)


class VersionNumberError(Exception): pass


def unify_rule_version(version):
    r"""
    unify version number from "a.b.c" to "a_b_c"

    >>> unify_rule_version('2_0_5')
    '2_0_5'
    >>> unify_rule_version('1.12.4')
    '1_12_4'
    >>> unify_rule_version('a____A')
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


def latest_version(names):
    r"""
    filter names according to version format , and get the latest one

    the version format is defined as "\d+_\d+_\d+"

    >>> latest_version(['2_9_1', '3_1_3', '1.3.4', '2_9', 'h_l_o' , 'ray_zhu'])
    '3_1_3'
    >>> latest_version(['2_9_1', '2_10_1'])
    '2_10_1'
    >>> latest_version(['ray_zhu'])
    Traceback (most recent call last):
        ...
    VersionNumberError: require at least one valid version
    >>> print(latest_version([]))
    Traceback (most recent call last):
        ...
    VersionNumberError: require at least one valid version
    """
    patt = re.compile(r'^(\d+)_(\d+)_(\d+)$')
    matches = ((name, patt.match(name)) for name in names)
    valid = (m for m in matches if m[1])

    try:
        latest_pair = max(valid, key=lambda m: [int(i) for i in m[1].groups()])
    except ValueError as e:
        raise VersionNumberError('require at least one valid version')

    latest = latest_pair[0]
    return latest


def get_schema_path(version):
    return abspath(pjoin(SCHEMA_FOLDER, version, MAIN_SCHEMA))


def check_schema(target, schema):
    """
    TBC

    return {'pass': True/False, 'report': obj}
    """
    return {'pass': True, 'report': ''}


def check_variable(target):
    """
    TBC

    return {'pass': True/False, 'report': obj}
    """
    return {'pass': True, 'report': ''}


def is_valid_jcf(jcf_path, version=None):
    """
    :param jcf_path: the absolute path of a JCF file
    :param version: the rule version
    """
    if version is None:
        version = latest_version(os.listdir(SCHEMA_FOLDER))

    version_ = unify_rule_version(version)
    schema_path = get_schema_path(version_)

    schema = json.load(open(schema_path))
    target = json.load(open(jcf_path))

    return all((
        check_schema(target, schema)['pass'],
        check_variable(target)['pass'],
        ))
