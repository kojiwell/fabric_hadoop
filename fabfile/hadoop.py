#!/usr/bin/env python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
 
import yaml
from fabric.api import task, run, sudo, put, task, \
        parallel, execute, env
from cuisine import file_exists, file_write, file_append, \
        text_strip_margin, mode_sudo, file_update, ssh_keygen, \
        ssh_authorize, dir_ensure

@task
def status():
    """ Check the status """
    yml_path = __file__.replace('fabfile','ymlfile').rstrip(r'\py|\pyc') + 'yml'
    f = open(yml_path)
    cfg = yaml.safe_load(f)
    f.close()

    env.user = cfg['remote_user']
    env.disable_known_hosts = True

    hosts = []
    for host in cfg['hosts']:
        hosts.append(cfg['hosts'][host]['ipaddr'])

    execute(check_status, hosts=hosts)

def check_status():

    sudo('jps', user='hdfs')

@task
def install():
    """ Install Hadoop Cluster """
    yml_path = __file__.replace('fabfile','ymlfile').rstrip(r'\py|\pyc') + 'yml'
    f = open(yml_path)
    cfg = yaml.safe_load(f)
    f.close()

    env.user = cfg['remote_user']
    env.disable_known_hosts = True

    hosts = []
    for host in cfg['hosts']:
        hosts.append(cfg['hosts'][host]['ipaddr'])

    execute(pkg_install,hosts=hosts)
    execute(update_etc_hosts,cfg_hosts=cfg['hosts'],hosts=hosts)
    execute(update_roles,cfg_hosts=cfg['hosts'],hosts=hosts)
    sites = ['core-site',
             'hdfs-site',
             'mapred-site']
    for site in sites:
        execute(update_config,cfg_name=site,cfg_list=cfg[site],hosts=hosts)
    execute(update_env_sh,hosts=hosts)
    admin_node = cfg['admin_node']
    admin_node_ip = cfg['hosts'][admin_node]['ipaddr']
    output = execute(create_hdfs_sshkey,hosts=[admin_node_ip])
    key = output[admin_node_ip]
    execute(update_authorized_keys,key=key,hosts=hosts)
    execute(update_dir,cfg['update_dir_list'],hosts=hosts)

@parallel
def update_dir(update_dir_list):

    with mode_sudo():
        for dir in update_dir_list:
            owner = update_dir_list[dir]['owner']
            mode = update_dir_list[dir]['mode']
            dir_ensure(dir, mode=mode, owner=owner)

@parallel
def update_authorized_keys(key):
    with mode_sudo():
        ssh_authorize(user='hdfs',key=key)

def create_hdfs_sshkey():
    with mode_sudo():
        ssh_keygen(user='hdfs',keytype='rsa')
        key = sudo('cat /usr/lib/hadoop/.ssh/id_rsa.pub')
    return key

@parallel
def update_env_sh():

    file = '/usr/lib/hadoop/conf/hadoop-env.sh'
    with mode_sudo():
        file_update(file, _sub_update_env_sh)

def _sub_update_env_sh(text):
    """ Update /usr/lib/hadoop/conf/hadoop-env.sh"""
    res = []
    for line in text.split('\n'):
        if line.strip().startswith("# export JAVA_HOME"):
            res.append("export JAVA_HOME=/usr/lib/jvm/java-7-oracle")
        else:
            res.append(line)
    return '\n'.join(res) + '\n'

@parallel
def update_config(cfg_name, cfg_list):
    """ Update xml files """

    lines = []
    header = text_strip_margin(
            """
            |<?xml version="1.0"?>
            |<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
            |
            |<!-- Put site-specific property overrides in this file. -->
            |
            |<configuration>
            |""")
    lines.append(header)

    for entry in cfg_list:
        property = text_strip_margin(
                """
                |  <property>
                |    <name>{0}</name>
                |    <value>{1}</value>
                |  </property>
                |""".format(entry, cfg_list[entry]))
        lines.append(property)

    footer = '</configuration>\n'
    lines.append(footer)

    file = '/usr/lib/hadoop/conf/' + cfg_name + '.xml'
    text = '\n'.join(lines) + '\n'
    file_write(file, text, sudo=True)

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

@task
@parallel
def enable_root_login():

    sudo('cat .ssh/authorized_keys > /root/.ssh/authorized_keys')

@parallel
def hello():

    run('hostname && id && echo hello')

