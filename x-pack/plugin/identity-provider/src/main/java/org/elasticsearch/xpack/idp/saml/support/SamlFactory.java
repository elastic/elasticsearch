/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.saml.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.hash.MessageDigests;
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
import java.security.SecureRandom;
import java.security.cert.CertificateEncodingException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.opensaml.core.xml.config.XMLObjectProviderRegistrySupport.getUnmarshallerFactory;

/**
 * Utility object for constructing new objects and values in a SAML 2.0 / OpenSAML context
 */
public class SamlFactory {

    private final XMLObjectBuilderFactory builderFactory;
    private final SecureRandom random;
    private static final Logger LOGGER = LogManager.getLogger(SamlFactory.class);

    public SamlFactory() {
        SamlInit.initialize();
        builderFactory = XMLObjectProviderRegistrySupport.getBuilderFactory();
        random = new SecureRandom();
    }

    public <T extends XMLObject> T object(Class<T> type, QName elementName) {
        final XMLObject obj = builderFactory.getBuilder(elementName).buildObject(elementName);
        return cast(type, elementName, obj);
    }

    public <T extends XMLObject> T object(Class<T> type, QName elementName, QName schemaType) {
        final XMLObject obj = builderFactory.getBuilder(schemaType).buildObject(elementName, schemaType);
        return cast(type, elementName, obj);
    }

    private <T extends XMLObject> T cast(Class<T> type, QName elementName, XMLObject obj) {
        if (type.isInstance(obj)) {
            return type.cast(obj);
        } else {
            throw new IllegalArgumentException("Object for element " + elementName.getLocalPart() + " is of type " + obj.getClass()
                + " not " + type);
        }
    }

    public String secureIdentifier() {
        return randomNCName(20);
    }

    private String randomNCName(int numberBytes) {
        final byte[] randomBytes = new byte[numberBytes];
        random.nextBytes(randomBytes);
        // NCNames (https://www.w3.org/TR/xmlschema-2/#NCName) can't start with a number, so start them all with "_" to be safe
        return "_".concat(MessageDigests.toHexString(randomBytes));
    }

    public <T extends XMLObject> T buildObject(Class<T> type, QName elementName) {
        final XMLObject obj = builderFactory.getBuilder(elementName).buildObject(elementName);
        if (type.isInstance(obj)) {
            return type.cast(obj);
        } else {
            throw new IllegalArgumentException("Object for element " + elementName.getLocalPart() + " is of type " + obj.getClass()
                + " not " + type);
        }
    }

    public String toString(Element element, boolean pretty) {
        try {
            StringWriter writer = new StringWriter();
            print(element, writer, pretty);
            return writer.toString();
        } catch (TransformerException e) {
            return "[" + element.getNamespaceURI() + "]" + element.getLocalName();
        }
    }

    public <T extends XMLObject> T buildXmlObject(Element element, Class<T> type) {
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

    void print(Element element, Writer writer, boolean pretty) throws TransformerException {
        final Transformer serializer = getHardenedXMLTransformer();
        if (pretty) {
            serializer.setOutputProperty(OutputKeys.INDENT, "yes");
        }
        serializer.transform(new DOMSource(element), new StreamResult(writer));
    }

    public String getXmlContent(SAMLObject object){
        return getXmlContent(object, false);
    }

    public String getXmlContent(SAMLObject object, boolean prettyPrint) {
        try {
            return toString(XMLObjectSupport.marshall(object), prettyPrint);
        } catch (MarshallingException e) {
            LOGGER.info("Error marshalling SAMLObject ", e);
            return "_unserializable_";
        }
    }

    public boolean elementNameMatches(Element element, String namespace, String localName) {
        return localName.equals(element.getLocalName()) && namespace.equals(element.getNamespaceURI());
    }

    public String text(Element dom, int length) {
        return text(dom, length, 0);
    }

    public String text(XMLObject xml, int prefixLength, int suffixLength) {
        final Element dom = xml.getDOM();
        if (dom == null) {
            return null;
        }
        return text(dom, prefixLength, suffixLength);
    }

    public String text(XMLObject xml, int length) {
        return text(xml, length, 0);
    }

    protected static String text(Element dom, int prefixLength, int suffixLength) {

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

    public String describeCredentials(Collection<? extends Credential> credentials) {
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

    public Element toDomElement(XMLObject object) {
        try {
            return XMLObjectSupport.marshall(object);
        } catch (MarshallingException e) {
            throw new ElasticsearchSecurityException("failed to marshall SAML object to DOM element", e);
        }
    }


    @SuppressForbidden(reason = "This is the only allowed way to construct a Transformer")
    public Transformer getHardenedXMLTransformer() throws TransformerConfigurationException {
        final TransformerFactory tfactory = TransformerFactory.newInstance();
        tfactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        tfactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        tfactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
        tfactory.setAttribute("indent-number", 2);
        Transformer transformer = tfactory.newTransformer();
        transformer.setErrorListener(new SamlFactory.TransformerErrorListener());
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
        documentBuilder.setErrorHandler(new SamlFactory.DocumentBuilderErrorHandler());
        return documentBuilder;
    }

    public String getJavaAlorithmNameFromUri(String sigAlg) {
        switch (sigAlg) {
            case "http://www.w3.org/2000/09/xmldsig#dsa-sha1":
                return "SHA1withDSA";
            case "http://www.w3.org/2000/09/xmldsig#dsa-sha256":
                return "SHA256withDSA";
            case "http://www.w3.org/2000/09/xmldsig#rsa-sha1":
                return "SHA1withRSA";
            case "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256":
                return "SHA256withRSA";
            case "http://www.w3.org/2001/04/xmldsig-more#ecdsa-sha256":
                return "SHA256withECDSA";
            default:
                throw new IllegalArgumentException("Unsupported signing algorithm identifier: " + sigAlg);
        }
    }


    private static String[] resolveSchemaFilePaths(String[] relativePaths) {

        return Arrays.stream(relativePaths).
            map(file -> {
                try {
                    return SamlFactory.class.getResource(file).toURI().toString();
                } catch (URISyntaxException e) {
                    LOGGER.warn("Error resolving schema file path", e);
                    return null;
                }
            }).filter(Objects::nonNull).toArray(String[]::new);
    }

    private static class DocumentBuilderErrorHandler implements org.xml.sax.ErrorHandler {
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

    private static class TransformerErrorListener implements javax.xml.transform.ErrorListener {

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

