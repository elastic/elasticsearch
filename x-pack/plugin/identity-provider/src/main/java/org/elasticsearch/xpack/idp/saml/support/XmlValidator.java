/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.support;

import org.elasticsearch.core.internal.io.IOUtils;
import org.w3c.dom.bootstrap.DOMImplementationRegistry;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSInput;
import org.w3c.dom.ls.LSResourceResolver;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

/**
 * Validates an XML stream against a specified schema.
 */
public class XmlValidator {

    private final SchemaFactory schemaFactory;
    private final String xsdName;

    public XmlValidator(String xsdName) {
        this.schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        this.xsdName = xsdName;
    }

    public void validate(String xml) throws Exception {
        try (InputStream stream = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))) {
            validate(stream);
        }
    }

    public void validate(InputStream xml) throws Exception {
        try (InputStream xsdStream = loadSchema(xsdName); ResourceResolver resolver = new ResourceResolver()) {
            schemaFactory.setResourceResolver(resolver);
            Schema schema = schemaFactory.newSchema(new StreamSource(xsdStream));
            Validator validator = schema.newValidator();
            validator.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            validator.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
            validator.validate(new StreamSource(xml));
        }
    }

    private InputStream loadSchema(String name) {
        if (name.endsWith(".xsd") && name.indexOf('/') == -1 && name.indexOf('\\') == -1) {
            return getClass().getResourceAsStream(name);
        } else {
            return null;
        }
    }

    private class ResourceResolver implements LSResourceResolver, AutoCloseable {
        private final DOMImplementationLS domLS;
        private final List<InputStream> streams;

        private ResourceResolver() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
            // Seriously, who thought this was a good idea for an API ???
            DOMImplementationRegistry registry = DOMImplementationRegistry.newInstance();
            domLS = (DOMImplementationLS) registry.getDOMImplementation("LS");
            streams = new ArrayList<>();
        }

        @Override
        public LSInput resolveResource(String type, String namespaceURI, String publicId, String systemId, String baseURI) {
            InputStream stream = loadSchema(systemId);
            if (stream == null) {
                return null;
            }
            streams.add(stream);
            final LSInput input = domLS.createLSInput();
            input.setByteStream(stream);
            return input;
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(streams);
        }
    }

}
