/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.XmlUtils;
import org.elasticsearch.xpack.core.security.support.RestorableContextClassLoader;
import org.opensaml.core.config.InitializationService;
import org.opensaml.core.xml.XMLObject;
import org.opensaml.core.xml.XMLObjectBuilderFactory;
import org.opensaml.core.xml.config.XMLObjectProviderRegistrySupport;
import org.opensaml.core.xml.io.MarshallingException;
import org.opensaml.core.xml.util.XMLObjectSupport;
import org.opensaml.saml.common.SAMLObject;
import org.opensaml.saml.saml2.core.Assertion;
import org.opensaml.saml.saml2.core.Response;
import org.opensaml.xmlsec.signature.impl.X509CertificateBuilder;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.bootstrap.DOMImplementationRegistry;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSInput;
import org.w3c.dom.ls.LSResourceResolver;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

public class SamlUtils {

    private static final String SAML_UNSOLICITED_RESPONSE_KEY = "es.security.saml.unsolicited_in_response_to";
    private static final String SAML_MARSHALLING_ERROR_STRING = "_unserializable_";

    private static final AtomicBoolean INITIALISED = new AtomicBoolean(false);
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private static XMLObjectBuilderFactory builderFactory = null;
    private static final Logger LOGGER = LogManager.getLogger(SamlUtils.class);

    /**
     * This is needed in order to initialize the underlying OpenSAML library.
     * It must be called before doing anything that potentially interacts with OpenSAML (whether in server code, or in tests).
     * The initialization happens within do privileged block as the underlying Apache XML security library has a permission check.
     * The initialization happens with a specific context classloader as OpenSAML loads resources from its jar file.
     */
    static void initialize(Logger logger) throws PrivilegedActionException {
        if (INITIALISED.compareAndSet(false, true)) {
            // We want to force these classes to be loaded _before_ we fiddle with the context classloader
            LoggerFactory.getLogger(InitializationService.class);
            SpecialPermission.check();
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                logger.debug("Initializing OpenSAML");
                try (RestorableContextClassLoader ignore = new RestorableContextClassLoader(InitializationService.class)) {
                    InitializationService.initialize();
                    // Force load this now, because it has a static field that needs to run inside the doPrivileged block
                    var ignore2 = new X509CertificateBuilder().buildObject();
                }
                logger.debug("Initialized OpenSAML");
                return null;
            });
            builderFactory = XMLObjectProviderRegistrySupport.getBuilderFactory();
        }
    }

    /**
     * Constructs exception for a specific case where the in-response-to value in the SAML content does not match any of the values
     * provided by the client. One example situation when this can happen is when user spent too much time on the IdP site, and meanwhile
     * the cookie storing the initial request id has expired (the default timeout is 2 minutes; this is the time in which browsers by
     * default allow sending cookies on requests originating from a different domain, which in this case means callback from IdP). In that
     * case the IdP would send a SAML response which content includes an in-response-to value matching the initial request id, however that
     * initial request id is now gone, so the client sends an empty in-response-to parameter causing a mismatch between the two.
     */
    static SamlAuthenticationException samlUnsolicitedInResponseToException(
        String samlContentInResponseTo,
        Collection<String> expectedInResponseTos
    ) {
        final SamlAuthenticationException exception = new SamlAuthenticationException(
            true,
            null,
            "SAML content is in-response-to [{}] but expected one of {}",
            samlContentInResponseTo,
            expectedInResponseTos
        );
        exception.addMetadata(SAML_UNSOLICITED_RESPONSE_KEY, samlContentInResponseTo);
        return exception;
    }

    public static <T extends XMLObject> T buildObject(Class<T> type, QName elementName) {
        final XMLObject obj = builderFactory.getBuilder(elementName).buildObject(elementName);
        if (type.isInstance(obj)) {
            return type.cast(obj);
        } else {
            throw new IllegalArgumentException(
                "Object for element " + elementName.getLocalPart() + " is of type " + obj.getClass() + " not " + type
            );
        }
    }

    public static String generateSecureNCName(int numberBytes) {
        final byte[] randomBytes = new byte[numberBytes];
        SECURE_RANDOM.nextBytes(randomBytes);
        // NCNames (https://www.w3.org/TR/xmlschema-2/#NCName) can't start with a number, so start them all with "_" to be safe
        return "_".concat(MessageDigests.toHexString(randomBytes));
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

    static String toString(Element element) {
        return toString(element, false);
    }

    static void print(Element element, Writer writer, boolean pretty) throws TransformerException {
        final Transformer serializer = XmlUtils.getHardenedXMLTransformer();
        if (pretty) {
            serializer.setOutputProperty(OutputKeys.INDENT, "yes");
        }
        serializer.transform(new DOMSource(element), new StreamResult(writer));
    }

    static String getXmlContent(SAMLObject object, boolean pretty) {
        try {
            return toString(XMLObjectSupport.marshall(object), pretty);
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
        return getXmlContent(object, true);
    }

    static void validate(InputStream xml, String xsdName) throws Exception {
        SchemaFactory schemaFactory = XmlUtils.getHardenedSchemaFactory();
        try (InputStream xsdStream = loadSchema(xsdName); ResourceResolver resolver = new ResourceResolver()) {
            schemaFactory.setResourceResolver(resolver);
            Schema schema = schemaFactory.newSchema(new StreamSource(xsdStream));
            Validator validator = XmlUtils.getHardenedValidator(schema);
            validator.validate(new StreamSource(xml));
        }
    }

    private static InputStream loadSchema(String name) {
        if (name.endsWith(".xsd") && name.indexOf('/') == -1 && name.indexOf('\\') == -1) {
            return SamlUtils.class.getResourceAsStream(name);
        } else {
            return null;
        }
    }

    private static class ResourceResolver implements LSResourceResolver, AutoCloseable {
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

    /**
     * Constructs a DocumentBuilder with all the necessary features for it to be secure
     *
     * @throws ParserConfigurationException if one of the features can't be set on the DocumentBuilderFactory
     */
    public static DocumentBuilder getHardenedBuilder(String[] schemaFiles) throws ParserConfigurationException {
        return XmlUtils.getHardenedBuilder(resolveSchemaFilePaths(schemaFiles));
    }

    private static String[] resolveSchemaFilePaths(String[] relativePaths) {

        return Arrays.stream(relativePaths).map(file -> {
            try {
                return SamlUtils.class.getResource(file).toURI().toString();
            } catch (URISyntaxException e) {
                LOGGER.warn("Error resolving schema file path", e);
                return null;
            }
        }).filter(Objects::nonNull).toArray(String[]::new);
    }
}
