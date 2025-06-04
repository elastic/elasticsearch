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
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.exception.ElasticsearchSecurityException;
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
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

public class SamlUtils {

    private static final String SAML_EXCEPTION_KEY = "es.security.saml";
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
     * Constructs an exception that can be distinguished (via {@link #isSamlException} as a SAML specific exception
     * Used to distinguish "expected" exceptions (such as SAML signature failures, or missing attributes) that should be treated as a
     * simple authentication failure (with a clear cause)
     */
    public static ElasticsearchSecurityException samlException(String msg, Object... args) {
        final ElasticsearchSecurityException exception = new ElasticsearchSecurityException(msg, args);
        exception.addMetadata(SAML_EXCEPTION_KEY);
        return exception;
    }

    /**
     * @see #samlException(String, Object...)
     */
    public static ElasticsearchSecurityException samlException(String msg, Exception cause, Object... args) {
        final ElasticsearchSecurityException exception = new ElasticsearchSecurityException(msg, cause, args);
        exception.addMetadata(SAML_EXCEPTION_KEY);
        return exception;
    }

    /**
     * @see #samlException(String, Object...)
     */
    public static boolean isSamlException(ElasticsearchSecurityException exception) {
        return exception != null && exception.getMetadata(SAML_EXCEPTION_KEY) != null;
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
        final Transformer serializer = getHardenedXMLTransformer();
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

    static void validate(InputStream xml, String xsdName) throws Exception {
        SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        try (InputStream xsdStream = loadSchema(xsdName); ResourceResolver resolver = new ResourceResolver()) {
            schemaFactory.setResourceResolver(resolver);
            Schema schema = schemaFactory.newSchema(new StreamSource(xsdStream));
            Validator validator = schema.newValidator();
            validator.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            validator.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
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
        dbf.setAttribute("http://java.sun.com/xml/jaxp/properties/schemaSource", resolveSchemaFilePaths(schemaFiles));
        DocumentBuilder documentBuilder = dbf.newDocumentBuilder();
        documentBuilder.setErrorHandler(new ErrorHandler());
        return documentBuilder;
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
}
