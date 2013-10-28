#!/usr/bin/env python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
 
import os
import sys
from fabric.api import *
from fabric.contrib import *
from fabric.operations import put, local
from cuisine import *

@task
@parallel
def install_common():
    ''':hostname - Install Hadoop Master'''
    env.user = 'ubuntu'
    env.disable_known_hosts = True

    file_name = '/usr/lib/jvm/java-7-oracle'
    if not file_exists(file_name):
        sudo('add-apt-repository -y ppa:webupd8team/java')
        sudo('add-apt-repository -y ppa:hadoop-ubuntu/stable')
        sudo('apt-get update && sudo apt-get -y upgrade')
        sudo('echo debconf shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections')
        sudo('echo debconf shared/accepted-oracle-license-v1-1 seen true | sudo debconf-set-selections')
        sudo('apt-get -y install oracle-java7-installer')
        sudo('apt-get -y install hadoop')
    else:
        print '{0} exists. Oracle Java is already installed.'.format(file_name)

    file_names = [
            '/etc/hosts',
            '/usr/lib/hadoop/conf/core-site.xml',
            '/usr/lib/hadoop/conf/hdfs-site.xml',
            '/usr/lib/hadoop/conf/mapred-site.xml',
            '/usr/lib/hadoop/conf/hadoop-env.sh',
            '/usr/lib/hadoop/conf/slaves'
            ]
    for file_name in file_names:
        put('templates'+ file_name, file_name, use_sudo=True)

    dir_name = '/var/lib/hdfs'
    if not file_exists(dir_name):
        sudo('mkdir {}'.format(dir_name))
        sudo('chown hdfs:hdfs {}'.format(dir_name))

    dir_name = '/root/.ssh'
    if not os.path.exists('templates' + dir_name):
        local('mkdir -p templates' + dir_name)
        local('ssh-keygen -f templates/{}/id_rsa -P \'\' -C \'root_key\''.format(dir_name))
        local('cat templates/{0}/id_rsa.pub > templates/{0}/authorized_keys'.format(dir_name))
        #local('echo StrictHostKeyChecking no > templates/{}/config'.format(dir_name))
        #local('echo UserKnownHostsFile=/dev/null >> templates/{}/config'.format(dir_name))

    if not file_exists(dir_name):
        sudo('mkdir {0} && chmod 700 {0}'.format(dir_name))
        put('templates/{}'.format(dir_name), '/root/', use_sudo=True, mode=0640)
        sudo('chmod 600 {}/id_rsa'.format(dir_name))
        sudo('chown -R root:root {}'.format(dir_name))

@task
def install_master():

    install_common()

@task
@parallel
def enable_root_login():

    sudo('cat .ssh/authorized_keys > /root/.ssh/authorized_keys')

@task
@parallel
def hello():

    run('hostname && id && echo hello')

@task
def nova_boot(tenant='fg368',
        image='futuregrid/ubuntu-12.04',
        flavor='m1.small',
        key_name='ktanaka_deigo',
        prefix='ktanaka',
        number=5):

    for a in range(number):
        local('supernova {0} boot \
                --image {1} \
                --flavor {2} \
                --key-name {3} \
                {4}{5}'.format(tenant,
                    image,
                    flavor,
                    key_name,
                    prefix,
                    a))
