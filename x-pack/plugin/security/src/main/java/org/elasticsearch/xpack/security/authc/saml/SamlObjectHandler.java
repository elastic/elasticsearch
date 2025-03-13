/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Streams;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.xpack.core.security.support.RestorableContextClassLoader;
import org.opensaml.core.xml.XMLObject;
import org.opensaml.core.xml.io.Unmarshaller;
import org.opensaml.core.xml.io.UnmarshallerFactory;
import org.opensaml.core.xml.io.UnmarshallingException;
import org.opensaml.saml.saml2.core.Issuer;
import org.opensaml.saml.saml2.encryption.Decrypter;
import org.opensaml.saml.security.impl.SAMLSignatureProfileValidator;
import org.opensaml.security.credential.Credential;
import org.opensaml.security.x509.X509Credential;
import org.opensaml.xmlsec.algorithm.AlgorithmSupport;
import org.opensaml.xmlsec.crypto.XMLSigningUtil;
import org.opensaml.xmlsec.encryption.support.ChainingEncryptedKeyResolver;
import org.opensaml.xmlsec.encryption.support.EncryptedKeyResolver;
import org.opensaml.xmlsec.encryption.support.InlineEncryptedKeyResolver;
import org.opensaml.xmlsec.encryption.support.SimpleKeyInfoReferenceEncryptedKeyResolver;
import org.opensaml.xmlsec.encryption.support.SimpleRetrievalMethodEncryptedKeyResolver;
import org.opensaml.xmlsec.keyinfo.KeyInfoCredentialResolver;
import org.opensaml.xmlsec.keyinfo.impl.ChainingKeyInfoCredentialResolver;
import org.opensaml.xmlsec.keyinfo.impl.CollectionKeyInfoCredentialResolver;
import org.opensaml.xmlsec.keyinfo.impl.LocalKeyInfoCredentialResolver;
import org.opensaml.xmlsec.keyinfo.impl.provider.DEREncodedKeyValueProvider;
import org.opensaml.xmlsec.keyinfo.impl.provider.InlineX509DataProvider;
import org.opensaml.xmlsec.keyinfo.impl.provider.KeyInfoReferenceProvider;
import org.opensaml.xmlsec.keyinfo.impl.provider.RSAKeyValueProvider;
import org.opensaml.xmlsec.signature.Signature;
import org.opensaml.xmlsec.signature.support.SignatureException;
import org.opensaml.xmlsec.signature.support.SignatureValidator;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import javax.xml.parsers.DocumentBuilder;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.security.authc.saml.SamlUtils.samlException;
import static org.opensaml.core.xml.config.XMLObjectProviderRegistrySupport.getUnmarshallerFactory;

public class SamlObjectHandler {

    protected static final String SAML_NAMESPACE = "urn:oasis:names:tc:SAML:2.0:protocol";

    private static final String[] XSD_FILES = new String[] {
        "/org/elasticsearch/xpack/security/authc/saml/saml-schema-protocol-2.0.xsd",
        "/org/elasticsearch/xpack/security/authc/saml/saml-schema-assertion-2.0.xsd",
        "/org/elasticsearch/xpack/security/authc/saml/xenc-schema.xsd",
        "/org/elasticsearch/xpack/security/authc/saml/xmldsig-core-schema.xsd" };

    private static final ThreadLocal<DocumentBuilder> THREAD_LOCAL_DOCUMENT_BUILDER = ThreadLocal.withInitial(() -> {
        try {
            return SamlUtils.getHardenedBuilder(XSD_FILES);
        } catch (Exception e) {
            throw samlException("Could not load XSD schema file", e);
        }
    });

    protected final Logger logger = LogManager.getLogger(getClass());

    @Nullable
    protected final Decrypter decrypter;

    private final Clock clock;
    private final IdpConfiguration idp;
    private final SpConfiguration sp;
    private final TimeValue maxSkew;
    private final UnmarshallerFactory unmarshallerFactory;

    public SamlObjectHandler(Clock clock, IdpConfiguration idp, SpConfiguration sp, TimeValue maxSkew) {
        this.clock = clock;
        this.idp = idp;
        this.sp = sp;
        this.maxSkew = maxSkew;
        this.unmarshallerFactory = getUnmarshallerFactory();
        if (sp.getEncryptionCredentials().isEmpty()) {
            this.decrypter = null;
        } else {
            this.decrypter = new Decrypter(null, createResolverForEncryptionKeys(), createResolverForEncryptedKeyElements());
        }
    }

    private KeyInfoCredentialResolver createResolverForEncryptionKeys() {
        final CollectionKeyInfoCredentialResolver collectionKeyInfoCredentialResolver = new CollectionKeyInfoCredentialResolver(
            Collections.unmodifiableCollection(sp.getEncryptionCredentials())
        );
        final LocalKeyInfoCredentialResolver localKeyInfoCredentialResolver = new LocalKeyInfoCredentialResolver(
            Arrays.asList(
                new InlineX509DataProvider(),
                new KeyInfoReferenceProvider(),
                new RSAKeyValueProvider(),
                new DEREncodedKeyValueProvider()
            ),
            collectionKeyInfoCredentialResolver
        );
        return new ChainingKeyInfoCredentialResolver(Arrays.asList(localKeyInfoCredentialResolver, collectionKeyInfoCredentialResolver));
    }

    private static EncryptedKeyResolver createResolverForEncryptedKeyElements() {
        return new ChainingEncryptedKeyResolver(
            Arrays.asList(
                new InlineEncryptedKeyResolver(),
                new SimpleRetrievalMethodEncryptedKeyResolver(),
                new SimpleKeyInfoReferenceEncryptedKeyResolver()
            )
        );
    }

    protected SpConfiguration getSpConfiguration() {
        return sp;
    }

    protected static String describe(X509Certificate certificate) {
        return "X509Certificate{Subject="
            + certificate.getSubjectX500Principal()
            + "; SerialNo="
            + certificate.getSerialNumber().toString(16)
            + "}";
    }

    protected static String describe(Collection<X509Credential> credentials) {
        return credentials.stream().map(credential -> describe(credential.getEntityCertificate())).collect(Collectors.joining(","));
    }

    void validateSignature(Signature signature) {
        final String signatureText = text(signature, 32);
        SAMLSignatureProfileValidator profileValidator = new SAMLSignatureProfileValidator();
        try {
            profileValidator.validate(signature);
        } catch (SignatureException e) {
            throw samlSignatureException(idp.getSigningCredentials(), signatureText, e);
        }

        checkIdpSignature(credential -> {
            try {
                final String signatureAlg = AlgorithmSupport.getKeyAlgorithm(signature.getSignatureAlgorithm());
                final String keyAlg = credential.getPublicKey().getAlgorithm();
                if (signatureAlg != null && signatureAlg.equals(keyAlg) == false) {
                    if (logger.isDebugEnabled()) {
                        String keyFingerprint = "SHA265:"
                            + MessageDigests.toHexString(MessageDigests.sha256().digest(credential.getPublicKey().getEncoded()));
                        logger.debug(
                            "Skipping [{}] key [{}] because it is not compatible with signature algorithm [{}]",
                            keyAlg,
                            keyFingerprint,
                            signatureAlg
                        );
                    }
                    return false;
                }
                return AccessController.doPrivileged((PrivilegedExceptionAction<Boolean>) () -> {
                    try (RestorableContextClassLoader ignore = new RestorableContextClassLoader(SignatureValidator.class)) {
                        SignatureValidator.validate(signature, credential);
                        logger.debug(
                            () -> format(
                                "SAML Signature [%s] matches credentials [%s] [%s]",
                                signatureText,
                                credential.getEntityId(),
                                credential.getPublicKey()
                            )
                        );
                        return true;
                    } catch (PrivilegedActionException e) {
                        logger.warn("SecurityException while attempting to validate SAML signature", e);
                        return false;
                    }
                });
            } catch (PrivilegedActionException e) {
                throw new SecurityException("SecurityException while attempting to validate SAML signature", e);
            }
        }, signatureText);
    }

    /**
     * Tests whether the provided function returns {@code true} for any of the IdP's signing credentials.
     * @throws ElasticsearchSecurityException - A SAML exception if not matching credential is found.
     */
    protected void checkIdpSignature(CheckedFunction<Credential, Boolean, Exception> check, String signatureText) {
        final Predicate<Credential> predicate = credential -> {
            try {
                return check.apply(credential);
            } catch (SignatureException | SecurityException e) {
                logger.debug(
                    () -> format(
                        "SAML Signature [%s] does not match credentials [%s] [%s] -- %s",
                        signatureText,
                        credential.getEntityId(),
                        credential.getPublicKey(),
                        e
                    )
                );
                logger.trace("SAML Signature failure caused by", e);
                return false;
            } catch (Exception e) {
                logger.warn("Exception while attempting to validate SAML Signature", e);
                return false;
            }
        };
        final List<Credential> credentials = idp.getSigningCredentials();
        if (credentials.stream().anyMatch(predicate) == false) {
            throw samlSignatureException(credentials, signatureText);
        }
    }

    /**
     * Constructs a SAML specific exception with a consistent message regarding SAML Signature validation failures
     */
    private ElasticsearchSecurityException samlSignatureException(List<Credential> credentials, String signature, Exception cause) {
        logger.warn(
            "The XML Signature of this SAML message cannot be validated. Please verify that the saml realm uses the correct SAML"
                + "metadata file/URL for this Identity Provider"
        );
        final String msg = "SAML Signature [{}] could not be validated against [{}]";
        return samlException(msg, cause, signature, describeCredentials(credentials));
    }

    private ElasticsearchSecurityException samlSignatureException(List<Credential> credentials, String signature) {
        logger.warn(
            "The XML Signature of this SAML message cannot be validated. Please verify that the saml realm uses the correct SAML"
                + "metadata file/URL for this Identity Provider"
        );
        final String msg = "SAML Signature [{}] could not be validated against [{}]";
        return samlException(msg, signature, describeCredentials(credentials));
    }

    private static String describeCredentials(List<Credential> credentials) {
        return credentials.stream().map(c -> {
            if (c == null) {
                return "<null>";
            }
            byte[] encoded;
            if (c instanceof X509Credential x) {
                try {
                    encoded = x.getEntityCertificate().getEncoded();
                } catch (CertificateEncodingException e) {
                    encoded = c.getPublicKey().getEncoded();
                }
            } else {
                encoded = c.getPublicKey().getEncoded();
            }
            return Base64.getEncoder().encodeToString(encoded).substring(0, 64) + "...";
        }).collect(Collectors.joining(","));
    }

    protected void checkIssuer(Issuer issuer, XMLObject parent) {
        if (issuer == null) {
            throw samlException(
                "Element {} ({}) has no issuer, but expected [{}]",
                parent.getElementQName(),
                text(parent, 16),
                idp.getEntityId()
            );
        }
        if (idp.getEntityId().equals(issuer.getValue()) == false) {
            throw samlException("SAML Issuer [{}] does not match expected value [{}]", issuer.getValue(), idp.getEntityId());
        }
    }

    protected long maxSkewInMillis() {
        return this.maxSkew.millis();
    }

    protected java.time.Instant now() {
        return clock.instant();
    }

    // Package private for testing
    <T extends XMLObject> T buildXmlObject(Element element, Class<T> type) {
        try {
            Unmarshaller unmarshaller = unmarshallerFactory.getUnmarshaller(element);
            if (unmarshaller == null) {
                throw samlException(
                    "XML element [{}] cannot be unmarshalled to SAML type [{}] (no unmarshaller)",
                    element.getTagName(),
                    type
                );
            }
            final XMLObject object = unmarshaller.unmarshall(element);
            if (type.isInstance(object)) {
                return type.cast(object);
            }
            Object[] args = new Object[] { element.getTagName(), type.getName(), object == null ? "<null>" : object.getClass().getName() };
            throw samlException("SAML object [{}] is incorrect type. Expected [{}] but was [{}]", args);
        } catch (UnmarshallingException e) {
            throw samlException("Failed to unmarshall SAML content [{}]", e, element.getTagName());
        }
    }

    protected static String text(XMLObject xml, int length) {
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

    protected Element parseSamlMessage(byte[] content) {
        final Element root;
        try (ByteArrayInputStream input = new ByteArrayInputStream(content)) {
            // This will parse and validate the input
            final Document doc = THREAD_LOCAL_DOCUMENT_BUILDER.get().parse(input);
            root = doc.getDocumentElement();
            if (logger.isTraceEnabled()) {
                logger.trace("Received SAML Message: {} \n", SamlUtils.toString(root, true));
            }
        } catch (SAXException | IOException e) {
            throw samlException("Failed to parse SAML message", e);
        }
        return root;
    }

    protected void validateNotOnOrAfter(Instant notOnOrAfter) {
        if (notOnOrAfter == null) {
            return;
        }
        final Instant now = now();
        final Instant pastNow = now.minusMillis(this.maxSkew.millis());
        if (pastNow.isBefore(notOnOrAfter) == false) {
            throw samlException("Rejecting SAML assertion because [{}] is on/after [{}]", pastNow, notOnOrAfter);
        }
    }

    protected ParsedQueryString parseQueryStringAndValidateSignature(String queryString, String samlMessageParameterName) {
        final String signatureInput = queryString.replaceAll("&Signature=.*$", "");
        final Map<String, String> parameters = new HashMap<>();
        RestUtils.decodeQueryString(queryString, 0, parameters);
        final String samlMessage = parameters.get(samlMessageParameterName);
        if (samlMessage == null) {
            throw samlException("Could not parse {} from query string: [{}]", samlMessageParameterName, queryString);
        }

        final String relayState = parameters.get("RelayState");
        final String signatureAlgorithm = parameters.get("SigAlg");
        final String signature = parameters.get("Signature");
        if (signature == null || signatureAlgorithm == null) {
            return new ParsedQueryString(samlMessage, false, relayState);
        }

        validateSignature(signatureInput, signatureAlgorithm, signature);
        return new ParsedQueryString(samlMessage, true, relayState);
    }

    private void validateSignature(String inputString, String signatureAlgorithm, String signature) {
        final byte[] sigBytes = decodeBase64(signature);
        final byte[] inputBytes = inputString.getBytes(StandardCharsets.US_ASCII);
        final String signatureText = Strings.cleanTruncate(signature, 32);
        checkIdpSignature(credential -> {
            if (XMLSigningUtil.verifyWithURI(credential, signatureAlgorithm, sigBytes, inputBytes)) {
                logger.debug(
                    () -> format(
                        "SAML Signature [%s] matches credentials [%s] [%s]",
                        signatureText,
                        credential.getEntityId(),
                        credential.getPublicKey()
                    )
                );
                return true;
            } else {
                logger.debug(
                    () -> format(
                        "SAML Signature [%s] failed against credentials [%s] [%s]",
                        signatureText,
                        credential.getEntityId(),
                        credential.getPublicKey()
                    )
                );
                return false;
            }
        }, signatureText);
    }

    protected byte[] decodeBase64(String content) {
        try {
            return Base64.getDecoder().decode(content.replaceAll("\\s+", ""));
        } catch (IllegalArgumentException e) {
            logger.info("Failed to decode base64 string [{}] - {}", content, e.toString());
            throw samlException("SAML message cannot be Base64 decoded", e);
        }
    }

    protected static byte[] inflate(byte[] bytes) {
        Inflater inflater = new Inflater(true);
        try (
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            InflaterInputStream inflate = new InflaterInputStream(in, inflater);
            ByteArrayOutputStream out = new ByteArrayOutputStream(bytes.length * 3 / 2)
        ) {
            Streams.copy(inflate, out);
            return out.toByteArray();
        } catch (IOException e) {
            throw samlException("SAML message cannot be inflated", e);
        }
    }

    static class ParsedQueryString {
        final String samlMessage;
        final boolean hasSignature;
        final String relayState;

        ParsedQueryString(String samlMessage, boolean hasSignature, String relayState) {
            this.samlMessage = samlMessage;
            this.hasSignature = hasSignature;
            this.relayState = relayState;
        }
    }
}
