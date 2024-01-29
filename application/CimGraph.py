from __future__ import annotations

from rdflib import Graph
import logging
import pathlib
import random
from io import BytesIO
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Mapping,
    NoReturn,
    Optional,
    Set,
    TextIO,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)
from urllib.parse import urlparse
from urllib.request import url2pathname

import rdflib.exceptions as exceptions
import rdflib.namespace as namespace  # noqa: F401 # This is here because it is used in a docstring.
import rdflib.plugin as plugin
import rdflib.query as query
import rdflib.util  # avoid circular dependency
from rdflib.collection import Collection
from rdflib.exceptions import ParserError
from rdflib.namespace import RDF, Namespace, NamespaceManager
from application.cimparser import InputSource, CIMParser, create_input_source
from rdflib.paths import Path
from rdflib.resource import Resource
from rdflib.serializer import Serializer
from rdflib.store import Store
from rdflib.term import (
    BNode,
    Genid,
    IdentifiedNode,
    Identifier,
    Literal,
    Node,
    RDFLibGenid,
    URIRef,
)

from application import cimplugin, cimparser


class CimGraph(Graph):
    def overridden_method(self, arg1, arg2):
        # Your custom implementation here
        pass

    def parse(
            self,
            source: Optional[
                Union[IO[bytes], TextIO, InputSource, str, bytes, pathlib.PurePath]
            ] = None,
            publicID: Optional[str] = None,  # noqa: N803
            format: Optional[str] = None,
            location: Optional[str] = None,
            file: Optional[Union[BinaryIO, TextIO]] = None,
            data: Optional[Union[str, bytes]] = None,
            datatypes_mapping: Optional[Dict[str, str]] = None,
            **args: Any,
    ) -> "Graph":
        """
        Parse an RDF source adding the resulting triples to the Graph.

        The source is specified using one of source, location, file or data.

        .. caution::

           This method can access directly or indirectly requested network or
           file resources, for example, when parsing JSON-LD documents with
           ``@context`` directives that point to a network location.

           When processing untrusted or potentially malicious documents,
           measures should be taken to restrict network and file access.

           For information on available security measures, see the RDFLib
           :doc:`Security Considerations </security_considerations>`
           documentation.

        :param source: An `InputSource`, file-like object, `Path` like object,
            or string. In the case of a string the string is the location of the
            source.
        :param location: A string indicating the relative or absolute URL of the
            source. `Graph`'s absolutize method is used if a relative location
            is specified.
        :param file: A file-like object.
        :param data: A string containing the data to be parsed.
        :param format: Used if format can not be determined from source, e.g.
            file extension or Media Type. Defaults to text/turtle. Format
            support can be extended with plugins, but "xml", "n3" (use for
            turtle), "nt" & "trix" are built in.
        :param publicID: the logical URI to use as the document base. If None
            specified the document location is used (at least in the case where
            there is a document location). This is used as the base URI when
            resolving relative URIs in the source document, as defined in `IETF
            RFC 3986
            <https://datatracker.ietf.org/doc/html/rfc3986#section-5.1.4>`_,
            given the source document does not define a base URI.
        :return: ``self``, i.e. the :class:`~rdflib.graph.Graph` instance.

        Examples:

        >>> my_data = '''
        ... <rdf:RDF
        ...   xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        ...   xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
        ... >
        ...   <rdf:Description>
        ...     <rdfs:label>Example</rdfs:label>
        ...     <rdfs:comment>This is really just an example.</rdfs:comment>
        ...   </rdf:Description>
        ... </rdf:RDF>
        ... '''
        >>> import os, tempfile
        >>> fd, file_name = tempfile.mkstemp()
        >>> f = os.fdopen(fd, "w")
        >>> dummy = f.write(my_data)  # Returns num bytes written
        >>> f.close()

        >>> g = Graph()
        >>> result = g.parse(data=my_data, format="application/rdf+xml")
        >>> len(g)
        2

        >>> g = Graph()
        >>> result = g.parse(location=file_name, format="application/rdf+xml")
        >>> len(g)
        2

        >>> g = Graph()
        >>> with open(file_name, "r") as f:
        ...     result = g.parse(f, format="application/rdf+xml")
        >>> len(g)
        2

        >>> os.remove(file_name)

        >>> # default turtle parsing
        >>> result = g.parse(data="<http://example.com/a> <http://example.com/a> <http://example.com/a> .")
        >>> len(g)
        3

        """

        source = create_input_source(
            source=source,
            publicID=publicID,
            location=location,
            file=file,
            data=data,
            format=format,
        )
        if format is None:
            format = source.content_type
        could_not_guess_format = False
        if format is None:
            if (
                    hasattr(source, "file")
                    and getattr(source.file, "name", None)
                    and isinstance(source.file.name, str)
            ):
                format = rdflib.util.guess_format(source.file.name)
            if format is None:
                format = "turtle"
                could_not_guess_format = True
        cimparser = cimplugin.get(format, CIMParser)()
        try:
            # TODO FIXME: Parser.parse should have **kwargs argument.
            cimparser.parse(source, self, **args)
        except SyntaxError as se:
            if could_not_guess_format:
                raise ParserError(
                    "Could not guess RDF format for %r from file extension so tried Turtle but failed."
                    "You can explicitly specify format using the format argument."
                    % source
                )
            else:
                raise se
        finally:
            if source.auto_close:
                source.close()
        return self
