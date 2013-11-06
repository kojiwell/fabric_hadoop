#!/usr/bin/env python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import os
import yaml
from fabric.api import task, local, settings, warn_only
from cuisine import file_exists

@task
def up():
    """ Boot instances """
    
    # call class OpenStack
    op = OpenStack()
    
    # Check if fingerprint exists on the list
    op.check_key(cfg)

class OpenStack:

    def __init__(self):

        cfg_dir = os.path.dirname(__file__).replace('fabfile','ymlfile')
        cfg_file = '{0}/{1}'.format(cfg_dir, 'openstack.yml')
        f = open(cfg_file)
        cfg = yaml.safe_load(f)
        cfg['key_file'] = os.path.abspath(os.path.expanduser(cfg['key_file']))
        key_fingerprint = \
                local('ssh-keygen -l -f {}|awk \'{{print $2}}\''.format(key_file), capture=True)
        f.close()

    def check_key(self):

        if not os.path.exists(cfg['key_file']):
            print "{} doesn't exist".format(cfg['key_file'])
            exit(1)

        with settings(warn_only=True):
            output = local('nova keypair-list|grep {}'.format(key_fingerprint))
        if not output.return_code == 0:
            print "ERROR: your key is not registered yet."
            exit(1)
        if not output.split()[1] == cfg['key_name']:
            print "your key is already registered with a different name."
            exit(1)

    #def check_image(self):
    #    with settings(warn_only=True):
