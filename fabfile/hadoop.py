#!/usr/bin/env python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
 
import yaml
from fabric.api import task, run, sudo, put, task, \
        parallel, execute, env
from cuisine import file_exists, file_write, file_append, \
        text_strip_margin, mode_sudo

@task
def install():

    yml_path = __file__.replace('fabfile','ymlfile').rstrip(r'\py|\pyc') + 'yml'
    f = open(yml_path)
    cfg = yaml.safe_load(f)
    f.close()

    env.user = cfg['remote_user']
    env.disable_known_hosts = True

    hosts = []
    for host in cfg['hosts']:
        hosts.append(cfg['hosts'][host]['ipaddr'])

    #execute(pkg_install,hosts=hosts)
    #execute(update_etc_hosts,cfg_hosts=cfg['hosts'],hosts=hosts)
    #execute(update_roles,cfg_hosts=cfg['hosts'],hosts=hosts)
    execute(update_config,cfg_name='core-site',cfg_list=cfg['core-site'],hosts=hosts)

@task
@parallel
def update_config(cfg_name, cfg_list):
    """ Update xml files """
    print 'cfg_name = ' + cfg_name
    print 'cfg_list = ' + cfg_list

@task
@parallel
def update_etc_hosts(cfg_hosts):
    """Update /etc/hosts """

    file = '/etc/hosts'
    lines = []
    lines.append("127.0.0.1 localhost")
    for host in cfg_hosts:
        lines.append("{0} {1}".format(cfg_hosts[host]['ipaddr'], host))
    text = '\n'.join(lines) + '\n'
    file_write(file, text, sudo=True)

@task
@parallel
def update_roles(cfg_hosts):
    """ Update /usr/lib/hadoop/conf/[masters/slaves] """

    dir = '/usr/lib/hadoop/conf/'
    masters = []
    slaves = []
    for host in cfg_hosts:
        if cfg_hosts[host]['group'] == 'masters':
            masters.append(host)
        elif cfg_hosts[host]['group'] == 'slaves':
            slaves.append(host)
    # Update masters
    file = dir + 'masters'
    text = '\n'.join(masters) + '\n'
    file_write(file, text, sudo=True)
    # Update slaves
    file = dir + 'slaves'
    text = '\n'.join(slaves) + '\n'
    file_write(file, text, sudo=True)

@task
@parallel
def pkg_install():
    ''':hostname - Install Hadoop Master'''
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

    #file_names = [
    #        '/etc/hosts',
    #        '/usr/lib/hadoop/conf/core-site.xml',
    #        '/usr/lib/hadoop/conf/hdfs-site.xml',
    #        '/usr/lib/hadoop/conf/mapred-site.xml',
    #        '/usr/lib/hadoop/conf/hadoop-env.sh',
    #        '/usr/lib/hadoop/conf/slaves'
    #        ]
    #for file_name in file_names:
    #    put('templates'+ file_name, file_name, use_sudo=True)

@task
@parallel
def enable_root_login():

    sudo('cat .ssh/authorized_keys > /root/.ssh/authorized_keys')

@parallel
def hello():

    run('hostname && id && echo hello')

