#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import stat
import fuse
import base64
from errno import ENOENT
from oobjlib.connection import Connection
from oobjlib.component import Object

fuse.fuse_python_api = (0, 2)


class OerpFS(fuse.Fuse):
    """
    Fuse filesystem for simple OpenERP filestore access
    """

    def __init__(self, *args, **kwargs):
        super(OerpFS, self).__init__(*args, **kwargs)

        # Command line arguments
        self.parser.add_option(mountopt='treetype', default='model', help='Type of tree to display [Default: model]')
        self.parser.add_option(mountopt='oerp_server', default='localhost', help='Hostname of the OpenERP server to use')
        self.parser.add_option(mountopt='oerp_port', default='8069', help='Port used to connect to OpenERP')
        self.parser.add_option(mountopt='oerp_dbname', default='demo', help='OpenERP database name')
        self.parser.add_option(mountopt='oerp_user', default='admin', help='OpenERP username')
        self.parser.add_option(mountopt='oerp_passwd', default='admin', help='OpenERP user password')

        self.ROOT_DIRS = {
            'model': [self.readdir_model, self.getattr_model],
        }

    def fsinit(self):
        self.treetype = self.cmdline[0].treetype
        if self.treetype not in self.ROOT_DIRS:
            # TODO : Raise an error
            pass

        # Initialize OpenERP Connection
        self.server = self.cmdline[0].oerp_server
        self.port = self.cmdline[0].oerp_port
        self.dbname = self.cmdline[0].oerp_dbname
        self.login = self.cmdline[0].oerp_user
        self.password = self.cmdline[0].oerp_passwd
        self.oerp_connection = Connection(server=self.server, dbname=self.dbname, login=self.login, password=self.password, port=self.port)

    def getattr(self, path):
        if path == '/':
            fakeStat = fuse.Stat()
            fakeStat.st_mode = stat.S_IFDIR | 0400
            fakeStat.st_nlink = 0
            return fakeStat

        return self.ROOT_DIRS[self.treetype][1](path)

    def readdir(self, path, offset):
        yield fuse.Direntry('.')
        yield fuse.Direntry('..')

        for element in self.ROOT_DIRS[self.treetype][0](path, offset):
            yield fuse.Direntry(element)

    def getattr_model(self, path):
        fakeStat = fuse.Stat()
        fakeStat.st_mode = stat.S_IFDIR | 0400
        fakeStat.st_nlink = 0

        paths = path.split('/')[1:]
        if len(paths) > 3:
            return -ENOENT

        # Check for model existence
        model_obj = Object(self.oerp_connection, 'ir.model')
        model_ids = model_obj.search([('model', '=', paths[0])])
        if not model_ids:
            return -ENOENT
        elif len(paths) == 1:
            return fakeStat

        # Check for record existence
        element_obj = Object(self.oerp_connection, paths[0])
        element_ids = element_obj.search([('id', '=', int(paths[1]))])
        if not element_ids:
            return -ENOENT
        elif len(paths) == 2:
            return fakeStat

        # Chech for attachement existence
        attachment_obj = Object(self.oerp_connection, 'ir.attachment')
        attachment_ids = attachment_obj.search([('res_model', '=', paths[0]), ('res_id', '=', int(paths[1])), ('id', '=', self.id_from_label(paths[2]))])
        if not attachment_ids:
            return -ENOENT

        # Common stats
        fakeStat.st_mode = stat.S_IFREG | 0400
        fakeStat.st_nlink = 2

        # TODO : Read the file size from a dedicated field (created in a specific module)
        attachment_obj = Object(self.oerp_connection, 'ir.attachment')
        attachment_ids = attachment_obj.search([('res_model', '=', paths[0]), ('res_id', '=', int(paths[1])), ('id', '=', self.id_from_label(paths[2]))])
        attachment_data = attachment_obj.read(attachment_ids[0], ['datas'])
        fakeStat.st_size = len(base64.b64decode(attachment_data['datas']))
        return fakeStat

    def readdir_model(self, path, offset):
        paths = path.split('/')[1:]

        # List models
        if path == '/':
            model_obj = Object(self.oerp_connection, 'ir.model')
            model_ids = model_obj.search([])
            for model_data in model_obj.read(model_ids, ['model']):
                yield model_data['model']
        # List records
        elif len(paths) == 1:
            element_obj = Object(self.oerp_connection, paths[0])
            element_ids = element_obj.search([])
            for element_data in element_obj.read(element_ids, ['id']):
                yield str(element_data['id'])
        # List attachments
        else:
            attachment_obj = Object(self.oerp_connection, 'ir.attachment')
            attachment_ids = attachment_obj.search([('res_model', '=', paths[0]), ('res_id', '=', int(paths[1]))])
            for attachment_data in attachment_obj.read(attachment_ids, ['name']):
                yield '%d-%s' % (attachment_data['id'], attachment_data['name'])

    def read(self, path, size, offset):
        paths = path.split('/')[1:]
        # TODO : Create a module that allows to read files by slides
        attachment_obj = Object(self.oerp_connection, 'ir.attachment')
        attachment_ids = attachment_obj.search([('res_model', '=', paths[0]), ('res_id', '=', int(paths[1])), ('id', '=', self.id_from_label(paths[2]))])
        attachment_data = attachment_obj.read(attachment_ids[0], ['datas'])
        return base64.b64decode(attachment_data['datas'])[offset:size]

    def id_from_label(self, label):
        return int(label.split('-')[0])

if __name__ == '__main__':
    fs = OerpFS()
    fs.fuse_args.mountpoint = sys.argv[1]
    fs.multithreaded = False
    fs.parse(errex=1)
    #fs.fuse_args.add('debug')
    fs.main()

# vim:expandtab:smartindent:tabstop=4:softtabstop=4:shiftwidth=4:
