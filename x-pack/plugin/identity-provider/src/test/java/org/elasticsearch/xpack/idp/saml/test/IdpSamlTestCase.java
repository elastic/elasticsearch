/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.test;

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.FileMatchers;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.PemUtils;
import org.hamcrest.Matchers;
import org.opensaml.core.xml.XMLObject;
import org.opensaml.core.xml.io.MarshallingException;
import org.opensaml.core.xml.io.Unmarshaller;
import org.opensaml.core.xml.io.UnmarshallerFactory;
import org.opensaml.core.xml.io.UnmarshallingException;
import org.opensaml.core.xml.util.XMLObjectSupport;
import org.opensaml.security.x509.BasicX509Credential;
import org.opensaml.security.x509.X509Credential;
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
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opensaml.core.xml.config.XMLObjectProviderRegistrySupport.getUnmarshallerFactory;

public abstract class IdpSamlTestCase extends ESTestCase {

    protected List<X509Credential> readCredentials() throws CertificateException, IOException {
        List<X509Credential> list = new ArrayList<>(2);
        list.add(readCredentials("RSA", 1024));
        list.add(readCredentials("RSA", 2048));
        Collections.shuffle(list, random());
        return list;
    }

    protected X509Credential readCredentials(String type, int size) throws CertificateException, IOException {
        Path certPath = getDataPath("/keypair/keypair_" + type + "_" + size + ".crt");
        Path keyPath = getDataPath("/keypair/keypair_" + type + "_" + size + ".key");
        assertThat(certPath, FileMatchers.isRegularFile());
        assertThat(keyPath, FileMatchers.isRegularFile());

        final X509Certificate[] certificates = CertParsingUtils.readX509Certificates(List.of(certPath));
        assertThat("Incorrect number of certificates in " + certPath, certificates, Matchers.arrayWithSize(1));

        final PrivateKey privateKey = PemUtils.readPrivateKey(keyPath, () -> new char[0]);
        return new BasicX509Credential(certificates[0], privateKey);
    }

    protected <T extends XMLObject> T domElementToXmlObject(Element element, Class<T> type) throws UnmarshallingException {
        final UnmarshallerFactory unmarshallerFactory = getUnmarshallerFactory();
        Unmarshaller unmarshaller = unmarshallerFactory.getUnmarshaller(element);
        assertThat(unmarshaller, Matchers.notNullValue());
        final XMLObject object = unmarshaller.unmarshall(element);
        assertThat(object, Matchers.instanceOf(type));
        return type.cast(object);
    }

    protected void print(Element element, Writer writer, boolean pretty) throws TransformerException {
        final Transformer serializer = getHardenedXMLTransformer();
        if (pretty) {
            serializer.setOutputProperty(OutputKeys.INDENT, "yes");
        }
        serializer.transform(new DOMSource(element), new StreamResult(writer));
    }

    protected String toString(XMLObject object) {
        try {
            return toString(XMLObjectSupport.marshall(object));
        } catch (MarshallingException e) {
            throw new RuntimeException("cannot marshall XML object to DOM", e);
        }
    }

    protected String toString(Element element) {
        try (StringWriter writer = new StringWriter()) {
            print(element, writer, true);
            return writer.toString();
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
