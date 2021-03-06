#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import csv
import stat
import fuse
import base64
from errno import ENOENT
from StringIO import StringIO
from oobjlib.connection import Connection
from oobjlib.component import Object

fuse.fuse_python_api = (0, 2)


class OerpFS(fuse.Fuse):
    """
    Base class for OpenERP usage through fuse
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

    def fsinit(self):
        # Initialize OpenERP Connection
        self.server = self.cmdline[0].oerp_server
        self.port = self.cmdline[0].oerp_port
        self.dbname = self.cmdline[0].oerp_dbname
        self.login = self.cmdline[0].oerp_user
        self.password = self.cmdline[0].oerp_passwd
        self.oerp_connection = Connection(server=self.server, dbname=self.dbname, login=self.login, password=self.password, port=self.port)

    def getSubclassInstance(self):
        subclasses = {
            'model': OerpFSModel,
            'csvimport': OerpFSCsvImport,
        }

        return subclasses[self.cmdline[0].treetype]()


class OerpFSModel(OerpFS):
    """
    Fuse filesystem for simple OpenERP filestore access
    """
    def __init__(self, *args, **kwargs):
        super(OerpFSModel, self).__init__(*args, **kwargs)

    def getattr(self, path):
        """
        Return attributes for the specified path :
            - Search for the model as first part
            - Search for an existing record as second part
            - Search for an existing attachment as third part
            - There cannot be more than 3 parts in the path
        """
        fakeStat = fuse.Stat()
        fakeStat.st_mode = stat.S_IFDIR | 0400
        fakeStat.st_nlink = 0

        if path == '/':
            return fakeStat

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
        attachment_data = attachment_obj.read(attachment_ids, ['datas'])
        fakeStat.st_size = len(base64.b64decode(attachment_data[0]['datas']))
        return fakeStat

    def readdir(self, path, offset):
        """
        Return content of a directory :
            - List models for root path
            - List records for a model
            - List attachments for a record
        We don't have to check for the path, because getattr already returns -ENOENT if the model/record/attachment doesn't exist
        """
        yield fuse.Direntry('.')
        yield fuse.Direntry('..')

        paths = path.split('/')[1:]
        # List models
        if path == '/':
            model_obj = Object(self.oerp_connection, 'ir.model')
            model_ids = model_obj.search([])
            for model_data in model_obj.read(model_ids, ['model']):
                yield fuse.Direntry(model_data['model'])
        # List records
        elif len(paths) == 1:
            element_obj = Object(self.oerp_connection, paths[0])
            element_ids = element_obj.search([])
            for element_data in element_obj.read(element_ids, ['id']):
                yield fuse.Direntry(str(element_data['id']))
        # List attachments
        else:
            attachment_obj = Object(self.oerp_connection, 'ir.attachment')
            attachment_ids = attachment_obj.search([('res_model', '=', paths[0]), ('res_id', '=', int(paths[1]))])
            for attachment_data in attachment_obj.read(attachment_ids, ['name']):
                yield fuse.Direntry('%d-%s' % (attachment_data['id'], attachment_data['name']))

    def read(self, path, size, offset):
        """
        Return the specified slide of a file
        Note : Only the beginning of the name is required (the ID of the attachment), we can put anything after the first '-', it will be ignored
        """
        paths = path.split('/')[1:]
        # TODO : Create a module that allows to read files by slides
        attachment_obj = Object(self.oerp_connection, 'ir.attachment')
        attachment_ids = attachment_obj.search([('res_model', '=', paths[0]), ('res_id', '=', int(paths[1])), ('id', '=', self.id_from_label(paths[2]))])
        attachment_data = attachment_obj.read(attachment_ids, ['datas'])
        return base64.b64decode(attachment_data[0]['datas'])[offset:offset + size]

    def id_from_label(self, label):
        """
        Return the attachment ID from a file name : only the part before the first '-'
        """
        return int(label.split('-')[0])


class OerpFSCsvImport(OerpFS):
    """
    Automatic CSV import to OpenERP on file copy
    """
    def __init__(self, *args, **kwargs):
        super(OerpFSCsvImport, self).__init__(*args, **kwargs)

        # Dict used to store files contents
        self.files = {}

    def getattr(self, path):
        """
        Only the root path exists, where we copy the CSV files to be imported
        """
        fakeStat = fuse.Stat()
        fakeStat.st_mode = stat.S_IFDIR | 0200
        fakeStat.st_nlink = 0

        if path == '/':
            return fakeStat

        if path in self.files:
            fakeStat.st_mode = stat.S_IFREG | 0200
            fakeStat.st_nlink = 1
            return fakeStat

        return -ENOENT

    def readdir(self, path, offset):
        """
        As only the root path exists, we only have to return the default entries
        """
        yield fuse.Direntry('.')
        yield fuse.Direntry('..')

        for path in self.files:
            yield(fuse.Direntry(path))

    def open(self, path, flags):
        return 0

    def create(self, path, mode, fi=None):
        self.files[path] = StringIO()
        return 0

    def write(self, path, buf, offset):
        """
        Write the contents of a CSV file : store it in a variable
        """
        if not path in self.files:
            return -ENOENT
        self.files[path].write(buf)
        return len(buf)

    def flush(self, path):
        return 0

    def truncate(self, path, length):
        return 0

    def chmod(self, path):
        return 0

    def chown(self, path):
        return 0

    def utime(self, path, times=None):
        return 0

    def release(self, path, fh):
        """
        Writing of the file is finished, import the contents into OpenERP
        """
        # FIXME : Don't know why it doesn't work without rebuilding the StringIO object...
        value = StringIO(self.files[path].getvalue())

        # Parse the CSV file contents
        csvFile = csv.reader(value)
        lines = list(csvFile)

        # Import data into OpenERP
        model = path.replace('.csv', '')[1:]
        oerpObject = Object(self.oerp_connection, model)
        oerpObject.import_data(lines[0], lines[1:], 'init', '', False, {'import': True})

        # Close StringIO and free memory
        self.files[path].close()
        del self.files[path]
        value.close()
        del value
        return True


if __name__ == '__main__':
    # First create an OerpFS instance, to parse the command line arguments, then ask for the good classe's instance
    tmpfs = OerpFS()
    tmpfs.parse(errex=1)
    fs = tmpfs.getSubclassInstance()
    # Our first instance is now useless, destroy it
    del tmpfs
    fs.fuse_args.mountpoint = sys.argv[1]
    fs.multithreaded = False
    fs.parse(errex=1)
    #fs.fuse_args.add('debug')
    fs.main()

# vim:expandtab:smartindent:tabstop=4:softtabstop=4:shiftwidth=4:
