# Licensed under the Mozilla Public License 2.0.
# Copyright(c) 2023, gridDigIt Kft. All rights reserved.
# author: Chavdar Ivanov

import io
import os
import sys
import zipfile
from urllib.parse import urlparse
import requests
import rdflib.term
from PyQt5.QtWidgets import *
from pyshacl import validate
from rdflib import Graph, RDFS, RDF, OWL

from application import gui


class ModShape(QDialog, gui.Ui_Dialog):
    def __init__(self, parent=None):
        super(ModShape, self).__init__(parent)
        self.setupUi(self)
        global project_path

        project_path = ""

        self.file_types = {
            'Instance Data': {'filter': "Instance data, (*.xml *.zip *.cimx)", 'button': self.InstanceData,
                              'label': self.labelSelectData},
            'SHACL Constraints': {'filter': "SHACL Constraints, *.ttl", 'button': self.SHACLConstraints,
                                  'label': self.labelSelectDataSHACL},
            'RDF Datatypes': {'filter': "RDF datatypes, *.rdf", 'button': self.RDFDatatypes,
                              'label': self.labelSelectRDFmap}
        }

        for action, details in self.file_types.items():
            self.connect_button(details['button'], action, details['filter'], details['label'])

        self.buttonOK.clicked.connect(lambda: self.push_button_ok())

        self.merged_instance_graph = Graph()
        self.merged_shacl_graph = Graph()
        self.merged_datatype_graph = Graph()

    def connect_button(self, button, action, file_filter, label):
        button.clicked.connect(lambda: self.select_file(action, project_path, file_filter, label))

    def select_file(self, action, pathi, file_filter, letext):
        selection = QFileDialog.getOpenFileNames(self, f"Please select the {action} file", pathi, file_filter)[0]

        if selection:
            letext.setText('\n'.join(selection))
            # Store the selection in a dictionary or any other data structure
            self.file_types[action]['selection'] = selection

    @staticmethod
    def get_format_from_extension(extension):
        # Map file extensions to RDF serialization formats
        extension_format_mapping = {
            '.xml': 'xml',
            '.rdf': 'xml',
            '.ttl': 'turtle',
            # Add more mappings as needed
        }
        return extension_format_mapping.get(extension, None)

    @staticmethod
    def is_supported_archive(extension):
        # Check if the file extension corresponds to a supported archive format
        supported_archive_extensions = ['.zip', '.cimx']  # Add more extensions as needed
        return extension.lower() in supported_archive_extensions

    def process_entry_content(self, entry_name, content):
        # Process the content and add triples to the merged graph
        # Determine the format based on the file extension
        _, ext = os.path.splitext(entry_name)
        ext = ext.lower()  # Convert to lowercase for case-insensitive comparison
        if self.is_supported_archive(ext):
            # If it's a supported archive format, treat it as a zip file
            with zipfile.ZipFile(io.BytesIO(content), 'r') as zip_ref:
                for member in zip_ref.infolist():
                    with zip_ref.open(member.filename) as inner_entry_file:
                        inner_content = inner_entry_file.read()
                        self.process_entry_content(member.filename, inner_content)
        else:
            # If it's not a supported archive format, treat it as a regular file
            file_format = self.get_format_from_extension(ext)
            if file_format:
                local_graph = Graph()
                local_graph.parse(data=content, format=file_format)
                self.merged_instance_graph = self.merged_instance_graph + local_graph

    def process_instance_data_contents(self, file_paths):
        for file_path in file_paths:
            with open(file_path, 'rb') as file:
                content = file.read()
                self.process_entry_content(file_path, content)

    def push_button_ok(self):  # button "OK"

        datatype_mapping = []
        instance_data_files = []
        shacl_file = []
        for action, details in self.file_types.items():
            if action == 'Instance Data':
                instance_data_files = details.get('selection', [])
            elif action == 'SHACL Constraints':
                shacl_file = details.get('selection', [])
            elif action == 'RDF Datatypes':
                datatype_mapping = details.get('selection', [])

        for file in datatype_mapping:
            local_datatype_mapping_graph = Graph()
            local_datatype_mapping_graph.parse(file, format='xml')
            self.merged_datatype_graph = self.merged_datatype_graph + local_datatype_mapping_graph

        self.process_instance_data_contents(instance_data_files)

        for file in shacl_file:
            #local_shacl_data_graph = Graph()
            #local_shacl_data_graph.parse(file, format='turtle')
            # check if the file has owl:imports and import nested owl:imports
            local_shacl_data_graph = self.load_owl_imports(file, visited_files=None)
            self.merged_shacl_graph = self.merged_shacl_graph + local_shacl_data_graph

        # this below on the dataset is a trial. maybe useful when we do multiple graphs and combining graphs
        # instanceDataDataset = Dataset()  # 1st define the graph
        # instanceDataDataset.parse(instanceDataFile,format="xml",publicID=instanceDataDataset.default_context.identifier)
        #
        # shaclDataDataset = Dataset()  # 1st define the graph
        # shaclDataDataset.parse(shaclFile,format="turtle",publicID=shaclDataDataset.default_context.identifier)

        # as the inference somehow does not work, here another way to add the datatypes

        graph_literals = self.merged_instance_graph.query(
            """SELECT DISTINCT ?p
                WHERE {
                    ?s ?p ?o
                    FILTER isLiteral(?o)
                }"""
        )

        # then loop on the literals and if the literal is in tha datatype map then add teh datatype
        for literal in graph_literals:
            datatype = self.merged_datatype_graph.triples((literal[0], RDFS.range, None))
            for s_d, p_d, o_d in datatype:
                # print(f"The datatype of {s_d} is {o_d}")
                for s_i, p_i, o_i in self.merged_instance_graph.triples((None, s_d, None)):
                    # print(f"{s_i} has predicate {p_i} which is {o_i}")
                    self.merged_instance_graph.add((s_i, p_i, rdflib.Literal(o_i,
                                                                             datatype=o_d.__str__())))  # this triggers a warning that can be ignorred
                    self.merged_instance_graph.remove((s_i, p_i, o_i))

        # this is validation without inference
        r = validate(self.merged_instance_graph,
                     shacl_graph=self.merged_shacl_graph,
                     ont_graph=None,
                     inference='none',
                     abort_on_first=False,
                     allow_infos=False,
                     allow_warnings=False,
                     meta_shacl=False,
                     advanced=False,
                     js=False,
                     debug=False,
                     do_owl_imports=False)

        conforms, results_graph, results_text = r

        file_name = os.path.normpath(QFileDialog.getSaveFileName(self, "xxx", "C:", "*.jsonld")[0])

        results_graph.serialize(destination=file_name, format='json-ld')
        print(f'{"Validation finished"}')
        print(f'{conforms}')

    @staticmethod
    def is_url(path):
        # Check if the path starts with "http://" or "https://"
        return path.startswith(("http://", "https://"))

    def load_owl_imports(self, file_path, visited_files=None):
        # If visited_files is not provided, create a new set
        if visited_files is None:
            visited_files = set()
        # Create an RDF graph
        local_graph = Graph()

        # Parse the main file
        if self.is_url(file_path):
            # Fetch the file content from the URL
            response = requests.get(file_path)
            response.raise_for_status()
            local_graph.parse(data=response.text, format='turtle')
        else:
            # Parse the local file
            local_graph.parse(file_path, format='turtle')

        # Check for owl:imports triples
        imports = local_graph.objects(predicate=OWL.imports)

        for import_uri in imports:
            import_path = str(import_uri)

            # Avoid infinite recursion by checking if the file has already been visited
            if import_path not in visited_files:
                visited_files.add(import_path)

                # Parse the imported file
                imported_graph = self.load_owl_imports(import_path, visited_files)

                # Merge the imported graph into the main graph
                local_graph = local_graph + imported_graph

        return local_graph


def main():
    qt_app = QApplication.instance()  # reuse old Qt application
    if qt_app is None:
        qt_app = QApplication(sys.argv)  # create a new Qt application

    exitcode = 1
    try:
        form = ModShape()
        form.show()
        exitcode = qt_app.exec_()
    finally:
        sys.exit(exitcode)


if __name__ == '__main__':
    main()
