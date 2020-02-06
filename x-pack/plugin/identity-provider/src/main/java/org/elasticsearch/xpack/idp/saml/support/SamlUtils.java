/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.xpack.core.security.support.RestorableContextClassLoader;
import org.opensaml.core.config.InitializationService;
import org.opensaml.core.xml.XMLObject;
import org.opensaml.core.xml.XMLObjectBuilderFactory;
import org.opensaml.core.xml.config.XMLObjectProviderRegistrySupport;
import org.opensaml.core.xml.io.MarshallingException;
import org.opensaml.core.xml.io.Unmarshaller;
import org.opensaml.core.xml.io.UnmarshallerFactory;
import org.opensaml.core.xml.io.UnmarshallingException;
import org.opensaml.core.xml.util.XMLObjectSupport;
import org.opensaml.saml.common.SAMLObject;
import org.opensaml.security.credential.Credential;
import org.opensaml.security.x509.X509Credential;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

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
import java.io.StringWriter;
import java.io.Writer;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.security.cert.CertificateEncodingException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.opensaml.core.xml.config.XMLObjectProviderRegistrySupport.getUnmarshallerFactory;

public final class SamlUtils {

    private static final AtomicBoolean INITIALISED = new AtomicBoolean(false);
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final Logger LOGGER = LogManager.getLogger();
    private static XMLObjectBuilderFactory builderFactory = null;

    private SamlUtils() {
    }

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
        builderFactory = XMLObjectProviderRegistrySupport.getBuilderFactory();
    }

    public static String secureIdentifier() {
        return randomNCName(20);
    }

    private static String randomNCName(int numberBytes) {
        final byte[] randomBytes = new byte[numberBytes];
        SECURE_RANDOM.nextBytes(randomBytes);
        // NCNames (https://www.w3.org/TR/xmlschema-2/#NCName) can't start with a number, so start them all with "_" to be safe
        return "_".concat(MessageDigests.toHexString(randomBytes));
    }

    public static <T extends XMLObject> T buildObject(Class<T> type, QName elementName) {
        final XMLObject obj = builderFactory.getBuilder(elementName).buildObject(elementName);
        if (type.isInstance(obj)) {
            return type.cast(obj);
        } else {
            throw new IllegalArgumentException("Object for element " + elementName.getLocalPart() + " is of type " + obj.getClass()
                + " not " + type);
        }
    }

    public static String toString(Element element, boolean pretty) {
        try {
            StringWriter writer = new StringWriter();
            print(element, writer, pretty);
            return writer.toString();
        } catch (TransformerException e) {
            return "[" + element.getNamespaceURI() + "]" + element.getLocalName();
        }
    }

    public static <T extends XMLObject> T buildXmlObject(Element element, Class<T> type) {
        try {
            UnmarshallerFactory unmarshallerFactory = getUnmarshallerFactory();
            Unmarshaller unmarshaller = unmarshallerFactory.getUnmarshaller(element);
            if (unmarshaller == null) {
                throw new ElasticsearchSecurityException("XML element [{}] cannot be unmarshalled to SAML type [{}] (no unmarshaller)",
                    element.getTagName(), type);
            }
            final XMLObject object = unmarshaller.unmarshall(element);
            if (type.isInstance(object)) {
                return type.cast(object);
            }
            Object[] args = new Object[]{element.getTagName(), type.getName(), object.getClass().getName()};
            throw new ElasticsearchSecurityException("SAML object [{}] is incorrect type. Expected [{}] but was [{}]", args);
        } catch (UnmarshallingException e) {
            throw new ElasticsearchSecurityException("Failed to unmarshall SAML content [{}]", e, element.getTagName());
        }
    }

    static void print(Element element, Writer writer, boolean pretty) throws TransformerException {
        final Transformer serializer = getHardenedXMLTransformer();
        if (pretty) {
            serializer.setOutputProperty(OutputKeys.INDENT, "yes");
        }
        serializer.transform(new DOMSource(element), new StreamResult(writer));
    }

    static String samlObjectToString(SAMLObject object) {
        try {
            return toString(XMLObjectSupport.marshall(object), true);
        } catch (MarshallingException e) {
            LOGGER.info("Error marshalling SAMLObject ", e);
            return "_unserializable_";
        }
    }

    public static String text(XMLObject xml, int length) {
        return text(xml, length, 0);
    }

    protected static String text(XMLObject xml, int prefixLength, int suffixLength) {
        final Element dom = xml.getDOM();
        if (dom == null) {
            return null;
        }
        final String text = dom.getTextContent().trim();
        final int totalLength = prefixLength + suffixLength;
        if (text.length() > totalLength) {
            final String prefix = Strings.cleanTruncate(text, prefixLength) + "...";
            if (suffixLength == 0) {
                return prefix;
            }
            int suffixIndex = text.length() - suffixLength;
            if (Character.isHighSurrogate(text.charAt(suffixIndex))) {
                suffixIndex++;
            }
            return prefix + text.substring(suffixIndex);
        } else {
            return text;
        }
    }

    public static String describeCredentials(List<Credential> credentials) {
        return credentials.stream()
            .map(c -> {
                if (c == null) {
                    return "<null>";
                }
                byte[] encoded;
                if (c instanceof X509Credential) {
                    X509Credential x = (X509Credential) c;
                    try {
                        encoded = x.getEntityCertificate().getEncoded();
                    } catch (CertificateEncodingException e) {
                        encoded = c.getPublicKey().getEncoded();
                    }
                } else {
                    encoded = c.getPublicKey().getEncoded();
                }
                return Base64.getEncoder().encodeToString(encoded).substring(0, 64) + "...";
            })
            .collect(Collectors.joining(","));
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
        dbf.setAttribute("http://java.sun.com/xml/jaxp/properties/schemaLanguage",
            XMLConstants.W3C_XML_SCHEMA_NS_URI);
        // We ship our own xsd files for schema validation since we do not trust anyone else.
        dbf.setAttribute("http://java.sun.com/xml/jaxp/properties/schemaSource", resolveSchemaFilePaths(schemaFiles));
        DocumentBuilder documentBuilder = dbf.newDocumentBuilder();
        documentBuilder.setErrorHandler(new ErrorHandler());
        return documentBuilder;
    }

    private static String[] resolveSchemaFilePaths(String[] relativePaths) {

        return Arrays.stream(relativePaths).
            map(file -> {
                try {
                    return SamlUtils.class.getResource(file).toURI().toString();
                } catch (URISyntaxException e) {
                    LOGGER.warn("Error resolving schema file path", e);
                    return null;
                }
            }).filter(Objects::nonNull).toArray(String[]::new);
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
            warning(e);
        }

        @Override
        public void fatalError(SAXParseException e) throws SAXException {
            warning(e);
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
            warning(e);
        }

        @Override
        public void fatalError(TransformerException e) throws TransformerException {
            warning(e);
        }
    }

}
