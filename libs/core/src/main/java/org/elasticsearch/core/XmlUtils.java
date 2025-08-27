/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;

public class XmlUtils {

    private static final Logger LOGGER = LogManager.getLogger(XmlUtils.class);

    /**
     * Constructs a DocumentBuilder with all the necessary features for it to be secure
     *
     * @throws ParserConfigurationException if one of the features can't be set on the DocumentBuilderFactory
     */
    @SuppressForbidden(reason = "This is the only allowed way to construct a DocumentBuilder")
    public static DocumentBuilder getHardenedBuilder(String[] schemaFiles) throws ParserConfigurationException {
        final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        // Ensure that Schema Validation is enabled for the factory
        dbf.setValidating(true);
        // Disallow internal and external entity expansion
        dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
        dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        dbf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        dbf.setFeature("http://xml.org/sax/features/validation", true);
        dbf.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
        dbf.setIgnoringComments(true);
        // This is required, otherwise schema validation causes signature invalidation
        dbf.setFeature("http://apache.org/xml/features/validation/schema/normalized-value", false);
        // Make sure that URL schema namespaces are not resolved/downloaded from URLs we do not control
        dbf.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "file,jar");
        dbf.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "file,jar");
        dbf.setFeature("http://apache.org/xml/features/honour-all-schemaLocations", true);
        // Ensure we do not resolve XIncludes. Defaults to false, but set it explicitly to be future-proof
        dbf.setXIncludeAware(false);
        // Ensure we do not expand entity reference nodes
        dbf.setExpandEntityReferences(false);
        // Further limit danger from denial of service attacks
        dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        dbf.setAttribute("http://apache.org/xml/features/validation/schema", true);
        dbf.setAttribute("http://apache.org/xml/features/validation/schema-full-checking", true);
        dbf.setAttribute("http://java.sun.com/xml/jaxp/properties/schemaLanguage", XMLConstants.W3C_XML_SCHEMA_NS_URI);
        // We ship our own xsd files for schema validation since we do not trust anyone else.
        dbf.setAttribute("http://java.sun.com/xml/jaxp/properties/schemaSource", schemaFiles);
        DocumentBuilder documentBuilder = dbf.newDocumentBuilder();
        documentBuilder.setErrorHandler(new ErrorHandler());
        return documentBuilder;
    }

    @SuppressForbidden(reason = "This is the only allowed way to construct a Transformer")
    public static Transformer getHardenedXMLTransformer() throws TransformerConfigurationException {
        final TransformerFactory tfactory = TransformerFactory.newInstance();
        tfactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        tfactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        tfactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
        tfactory.setAttribute("indent-number", 2);
        Transformer transformer = tfactory.newTransformer();
        transformer.setErrorListener(new ErrorListener());
        return transformer;
    }

    private static class ErrorHandler implements org.xml.sax.ErrorHandler {
        /**
         * Enabling schema validation with `setValidating(true)` in our
         * DocumentBuilderFactory requires that we provide our own
         * ErrorHandler implementation
         *
         * @throws SAXException If the document we attempt to parse is not valid according to the specified schema.
         */
        @Override
        public void warning(SAXParseException e) throws SAXException {
            LOGGER.debug("XML Parser error ", e);
            throw e;
        }

        @Override
        public void error(SAXParseException e) throws SAXException {
            LOGGER.debug("XML Parser error ", e);
            throw e;
        }

        @Override
        public void fatalError(SAXParseException e) throws SAXException {
            LOGGER.debug("XML Parser error ", e);
            throw e;
        }
    }

    private static class ErrorListener implements javax.xml.transform.ErrorListener {

        @Override
        public void warning(TransformerException e) throws TransformerException {
            LOGGER.debug("XML transformation error", e);
            throw e;
        }

        @Override
        public void error(TransformerException e) throws TransformerException {
            LOGGER.debug("XML transformation error", e);
            throw e;
        }

        @Override
        public void fatalError(TransformerException e) throws TransformerException {
            LOGGER.debug("XML transformation error", e);
            throw e;
        }
    }
}
