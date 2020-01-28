/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.test;

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.opensaml.core.xml.XMLObject;
import org.opensaml.core.xml.io.MarshallingException;
import org.opensaml.core.xml.util.XMLObjectSupport;
import org.w3c.dom.Element;

import javax.xml.XMLConstants;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.io.Writer;

public abstract class IdpSamlTestCase extends ESTestCase {

    protected void print(Element element, Writer writer, boolean pretty) throws TransformerException {
        final Transformer serializer = getHardenedXMLTransformer();
        if (pretty) {
            serializer.setOutputProperty(OutputKeys.INDENT, "yes");
        }
        serializer.transform(new DOMSource(element), new StreamResult(writer));
    }

    protected String toString(XMLObject object) {
        try (StringWriter writer = new StringWriter()) {
            print(XMLObjectSupport.marshall(object), writer, true);
            return writer.toString();
        } catch (MarshallingException e) {
            throw new RuntimeException("cannot marshall XML object to DOM", e);
        } catch (TransformerException e) {
            throw new RuntimeException("cannot transform XML element to string", e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressForbidden(reason = "This is the only allowed way to construct a Transformer")
    private static Transformer getHardenedXMLTransformer() throws TransformerConfigurationException {
        final TransformerFactory tfactory = TransformerFactory.newInstance();
        tfactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        tfactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        tfactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
        tfactory.setAttribute("indent-number", 2);
        Transformer transformer = tfactory.newTransformer();
        transformer.setErrorListener(new ErrorListener());
        return transformer;
    }
    private static class ErrorListener implements javax.xml.transform.ErrorListener {

        @Override
        public void warning(TransformerException e) throws TransformerException {
            throw e;
        }

        @Override
        public void error(TransformerException e) throws TransformerException {
            throw e;
        }

        @Override
        public void fatalError(TransformerException e) throws TransformerException {
            throw e;
        }
    }
}
