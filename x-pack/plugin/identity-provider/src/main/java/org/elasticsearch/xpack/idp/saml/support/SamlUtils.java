/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.support;

import javax.xml.XMLConstants;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.io.Writer;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.xpack.core.security.support.RestorableContextClassLoader;
import org.opensaml.core.config.InitializationService;
import org.opensaml.core.xml.XMLObjectBuilderFactory;
import org.opensaml.core.xml.io.MarshallingException;
import org.opensaml.core.xml.util.XMLObjectSupport;
import org.opensaml.saml.common.SAMLObject;
import org.opensaml.saml.saml2.core.Assertion;
import org.opensaml.saml.saml2.core.Response;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

public class SamlUtils {

    private static final String SAML_MARSHALLING_ERROR_STRING = "_unserializable_";

    private static final AtomicBoolean INITIALISED = new AtomicBoolean(false);

    private static final Logger LOGGER = LogManager.getLogger(SamlUtils.class);

    /**
     * This is needed in order to initialize the underlying OpenSAML library.
     * It must be called before doing anything that potentially interacts with OpenSAML (whether in server code, or in tests).
     * The initialization happens within do privileged block as the underlying Apache XML security library has a permission check.
     * The initialization happens with a specific context classloader as OpenSAML loads resources from its jar file.
     */
    public static void initialize() {
        if (INITIALISED.compareAndSet(false, true)) {
            // We want to force these classes to be loaded _before_ we fiddle with the context classloader
            LoggerFactory.getLogger(InitializationService.class);
            SpecialPermission.check();
            try {
                AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                    LOGGER.debug("Initializing OpenSAML");
                    try (RestorableContextClassLoader ignore = new RestorableContextClassLoader(InitializationService.class)) {
                        InitializationService.initialize();
                    }
                    LOGGER.debug("Initialized OpenSAML");
                    return null;
                });
            } catch (PrivilegedActionException e) {
                throw new ElasticsearchSecurityException("failed to set context classloader for SAML IdP", e);
            }
        }
    }

    static String toString(Element element, boolean pretty) {
        try {
            StringWriter writer = new StringWriter();
            print(element, writer, pretty);
            return writer.toString();
        } catch (TransformerException e) {
            return "[" + element.getNamespaceURI() + "]" + element.getLocalName();
        }
    }

    static void print(Element element, Writer writer, boolean pretty) throws TransformerException {
        final Transformer serializer = getHardenedXMLTransformer();
        if (pretty) {
            serializer.setOutputProperty(OutputKeys.INDENT, "yes");
        }
        serializer.transform(new DOMSource(element), new StreamResult(writer));
    }

    public static String samlObjectToString(SAMLObject object) {
        try {
            return toString(XMLObjectSupport.marshall(object), true);
        } catch (MarshallingException e) {
            LOGGER.info("Error marshalling SAMLObject ", e);
            return SAML_MARSHALLING_ERROR_STRING;
        }
    }

    static String describeSamlObject(SAMLObject object) {
        if (Response.class.isInstance(object)) {
            Response response = (Response) object;
            StringBuilder sb = new StringBuilder();
            sb.append("SAML Response: [\n");
            sb.append("    Destination: ").append(response.getDestination()).append("\n");
            sb.append("    Response ID: ").append(response.getID()).append("\n");
            sb.append("    In response to: ").append(response.getInResponseTo()).append("\n");
            sb.append("    Response issued at:").append(response.getIssueInstant()).append("\n");
            if (response.getIssuer() != null) {
                sb.append("    Issuer: ").append(response.getIssuer().getValue()).append("\n");
            }
            sb.append("    Number of unencrypted Assertions: ").append(response.getAssertions().size()).append("\n");
            sb.append("    Number of encrypted Assertions: ").append(response.getEncryptedAssertions().size()).append("\n");
            sb.append("]");
            return sb.toString();

        } else if (Assertion.class.isInstance(object)) {
            Assertion assertion = (Assertion) object;
            StringBuilder sb = new StringBuilder();
            sb.append("SAML Assertion: [\n");
            sb.append("    Response ID: ").append(assertion.getID()).append("\n");
            sb.append("    Response issued at: ").append(assertion.getIssueInstant()).append("\n");
            if (assertion.getIssuer() != null) {
                sb.append("    Issuer: ").append(assertion.getIssuer().getValue()).append("\n");
            }
            sb.append("    Number of attribute statements: ").append(assertion.getAttributeStatements().size()).append("\n");
            sb.append("    Number of authentication statements: ").append(assertion.getAuthnStatements().size()).append("\n");
            sb.append("]");
            return sb.toString();
        }
        return samlObjectToString(object);
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
