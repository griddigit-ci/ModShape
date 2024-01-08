# GUI
from typing import Generator

import rdflib.term
from PyQt5.QtWidgets import *
import gui
import sys
import os
from rdflib import Graph, Dataset, RDFS, RDF
from pyshacl import validate


class SHACLvalidator(QDialog, gui.Ui_Dialog):
    def __init__(self, parent=None):
        super(SHACLvalidator, self).__init__(parent)
        self.setupUi(self)
        global projectPath

        projectPath = ""

        self.readInstanceData.clicked.connect(
            lambda: self.selectFile(projectPath, "Instance data, *.xml", self.labelSelectData,
                                    "Please select the xml file to validate..."))  # show an "Open" dialog box and returns the path to the selected file
        self.readSHACLConstraints.clicked.connect(
            lambda: self.selectFile(projectPath, "SHACL Constraints, *.ttl", self.labelSelectDataSHACL,
                                    "Please select the SHACL Constraints file"))  # show an "Open" dialog box and returns the path to the selected file
        self.pbDatatype.clicked.connect(
            lambda: self.selectFile(projectPath, "RDF datatypes, *.rdf", self.labelSelectRDFmap,
                                    "Please select the RDF datatype mapping file"))  # show an "Open" dialog box and returns the path to the selected file

        self.buttonOK.clicked.connect(lambda: self.pushButton_OK())

    def selectFile(self, pathi, filter, letext, title):
        fileName = os.path.normpath(QFileDialog.getOpenFileName(self, title, pathi, filter)[0])

        if fileName != ".":
            letext.setText(fileName)


    def pushButton_OK(self):  # button "OK"

        datatype_mapping = self.labelSelectRDFmap.text()
        instance_data_file = self.labelSelectData.text()
        shacl_file = self.labelSelectDataSHACL.text()

        # print(f'{instance_data_file}')
        # print(f'{shacl_file}')

        # import the xml file

        datatype_mapping_graph = Graph()  # 1st define the graph
        datatype_mapping_graph.parse(datatype_mapping,format='xml')

        instance_data_graph = Graph()  # 1st define the graph
        instance_data_graph.parse(instance_data_file,format='xml')

        shacl_data_graph = Graph()  # 1st define the graph
        shacl_data_graph.parse(shacl_file,format='turtle')

        # this below on the dataset is a trial. maybe useful when we do multiple graphs and combining graphs
        # instanceDataDataset = Dataset()  # 1st define the graph
        # instanceDataDataset.parse(instanceDataFile,format="xml",publicID=instanceDataDataset.default_context.identifier)
        #
        # shaclDataDataset = Dataset()  # 1st define the graph
        # shaclDataDataset.parse(shaclFile,format="turtle",publicID=shaclDataDataset.default_context.identifier)

        # as the inference somehow does not work, here another way to add the datatypes


        graph_literals = instance_data_graph.query(
            """SELECT DISTINCT ?p
                WHERE {
                    ?s ?p ?o
                    FILTER isLiteral(?o)
                }"""
        )

        # # Define the subject variable outside the query
        # user_input_subject = "http://example.org/subject1"
        #
        # # Parameterized SPARQL query with proper URI formatting
        # query_template = """
        # SELECT ?subject ?predicate ?object
        # WHERE {{
        #   ?subject ?predicate ?object .
        #   FILTER(?predicate = <{predicate}>)
        # }}
        # """
        #
        # # Format the query by substituting the subject variable
        # formatted_query = query_template.format(predicate=RDF.type)
        #
        # # Execute the formatted query
        # results = instance_data_graph.query(formatted_query)

        # then loop on the literals and if the literal is in tha datatype map then add teh datatype
        for literal in graph_literals:
            datatype = datatype_mapping_graph.triples((literal[0], RDFS.range, None))
            for s_d, p_d, o_d in datatype:
                # print(f"The datatype of {s_d} is {o_d}")
                for s_i, p_i, o_i in instance_data_graph.triples((None, s_d, None)):
                    # print(f"{s_i} has predicate {p_i} which is {o_i}")
                    instance_data_graph.add((s_i, p_i, rdflib.Literal(o_i, datatype=o_d.__str__()))) # this triggers a warning that can be ignorred
                    instance_data_graph.remove((s_i, p_i, o_i))



        # this is validation without inference
        r = validate(instance_data_graph,
                     shacl_graph=shacl_data_graph,
                     ont_graph=None,
                     inference='none',
                     abort_on_first=False,
                     allow_infos=False,
                     allow_warnings=False,
                     meta_shacl=False,
                     advanced=False,
                     js=False,
                     debug=False,
                     do_owl_imports=True)

        #this is validation with inference; Need to see why this does not work. Most probably the datatypes need to be defined differently
        # r = validate(instance_data_graph,
        #              shacl_graph=shacl_data_graph,
        #              ont_graph=datatype_mapping_graph,
        #              inference='both',
        #              abort_on_first=False,
        #              allow_infos=False,
        #              allow_warnings=False,
        #              meta_shacl=False,
        #              advanced=False,
        #              js=False,
        #              debug=False)

        conforms, results_graph, results_text = r

        fileName = os.path.normpath(QFileDialog.getSaveFileName(self, "xxx", "C:", ".jsonld")[0])

        results_graph.serialize(destination=fileName,format='json-ld')
        print(f'{"Validation finished"}')
        print(f'{conforms}')


def main():
    QtApp = QApplication.instance()  # reuse old Qt application
    if QtApp is None:
        QtApp = QApplication(sys.argv)  # create a new Qt application

    exitcode = 1
    try:
        # self.app.PrintPlain("Starting OSA AP2 GUI application ...")
        form = SHACLvalidator()
        form.show()
        exitcode = QtApp.exec_()
    finally:
        # self.app.PrintPlain("Destroying OSA AP2 GUI application ...")
        # form._destroy()

        sys.exit(exitcode)


if __name__ == '__main__':
    main()
