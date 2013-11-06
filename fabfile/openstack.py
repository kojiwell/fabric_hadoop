#!/usr/bin/env python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import os
import yaml
from fabric.api import task, local, settings, warn_only
from cuisine import file_exists

@task
def up():
    """ Boot instances """
    
    # Read config file
    cfg = read_ymlfile('openstack.yml')
    
    # Get absolute path of public key
    key_file = os.path.abspath(os.path.expanduser(cfg['key_file']))

    # Check if file exist
    if not os.path.exists(key_file):
        print "{} doesn't exist"
        exit(1)

    # Get fingerprint
    fingerprint = local('ssh-keygen -l -f {}|awk \'{{print $2}}\''.format(key_file), capture=True)

    # Check if fingerprint exists on the list
    with settings(warn_only=True):
        test = local('nova keypair-list|grep {}'.format(fingerprint))
    print test.return_code

def read_ymlfile(file_name):
    """ Check the status """
    # Read cofiguration file to cfg
    cfg_dir = os.path.dirname(__file__).replace('fabfile','ymlfile')
    cfg_file = '{0}/{1}'.format(cfg_dir, file_name)
    f = open(cfg_file)
    cfg = yaml.safe_load(f)
    f.close()
    
    return cfg
