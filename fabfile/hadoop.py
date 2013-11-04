#!/usr/bin/env python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
 
import yaml
from fabric.api import task, run, sudo, put, task, \
        parallel, execute, env
from cuisine import file_exists, file_write, file_append, \
        text_strip_margin

@task
def install():

    yml_path = __file__.replace('fabfile','ymlfile').rstrip(r'\py|\pyc') + 'yml'
    f = open(yml_path)
    cfg = yaml.safe_load(f)
    f.close()

    env.user = cfg['remote_user']

    hosts = []
    for host in cfg['hosts']:
        hosts.append(cfg['hosts'][host]['ipaddr'])

    execute(pkg_install,hosts=hosts)
    execute(update_etc_hosts,cfg_hosts=cfg['hosts'],hosts=hosts)

@task
@parallel
def update_etc_hosts(cfg_hosts):

    file = '/etc/hosts'
    text = text_strip_margin(
            """
            |127.0.0.1 localhost
            |""")
    file_write(file, text, sudo=True)
    for host in cfg_hosts:
        text = text_strip_margin(
                """
                |{0} {1}
                |""".format(cfg_hosts[host]['ipaddr'], host))
        file_append(file, text, sudo=True)

@task
@parallel
def pkg_install():
    ''':hostname - Install Hadoop Master'''
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

