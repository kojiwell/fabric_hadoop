#!/usr/bin/env python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import os
import yaml
from fabric.api import task, run, sudo, put, task, \
        parallel, execute, env
from cuisine import file_exists, file_write, file_append, \
        text_strip_margin, mode_sudo, file_update, ssh_keygen, \
        ssh_authorize, dir_ensure

@task
def up():
    """ Boot instances """
    
    # Read config file
    cfg = read_ymlfile('openstack.yml')
    
    # Get absolute path of public key
    key_file = os.path.abspath(cfg['key_file'])

    # Check if file exist
    if not os.path.exists(key_file):
        print "{} doesn't exist"
        exit(1)

    # Get fingerprint
    fingerprint = local('ssh-keygen -l -f {}|awk \'{{print $2}}\''.format(key_file))
    print "finger print is {}".format(fingerprint)

def read_ymlfile(file_name):
    """ Check the status """
    # Read cofiguration file to cfg
    cfg_dir = os.path.dirname(__file__).replace('fabfile','ymlfile')
    cfg_file = '{0}/{1}'.format(cfg_dir, file_name)
    f = open(cfg_file)
    cfg = yaml.safe_load(f)
    f.close()
    
    return cfg
