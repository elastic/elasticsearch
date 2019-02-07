/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.saml;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.core.internal.io.Streams;
import org.elasticsearch.rest.RestUtils;
import org.opensaml.saml.common.SAMLObject;
import org.opensaml.saml.saml2.core.EncryptedID;
import org.opensaml.saml.saml2.core.LogoutRequest;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.xmlsec.crypto.XMLSigningUtil;
import org.opensaml.xmlsec.encryption.support.DecryptionException;
import org.opensaml.xmlsec.signature.Signature;
import org.w3c.dom.Element;

import static org.elasticsearch.xpack.security.authc.saml.SamlUtils.samlException;

/**
 * Processes a LogoutRequest for an IdP-initiated logout.
 */
public class SamlLogoutRequestHandler extends SamlRequestHandler {

    private static final String REQUEST_TAG_NAME = "LogoutRequest";

    SamlLogoutRequestHandler(Clock clock, IdpConfiguration idp, SpConfiguration sp, TimeValue maxSkew) {
        super(clock, idp, sp, maxSkew);
    }

    /**
     * Processes the provided LogoutRequest and extracts the NameID and SessionIndex.
     * Returns these in a {@link SamlAttributes} object with an empty attributes list.
     * <p>
     * The recommended binding for Logout (for maximum interoperability) is HTTP-Redirect.
     * Under this binding the signature is applied to the query-string (including parameter
     * names and url-encoded/base64-encoded/deflated values). Therefore in order to properly
     * validate the signature, this method operates on a raw query- string.
     *
     * @throws ElasticsearchSecurityException If the SAML is invalid for this realm/configuration
     */
    public Result parseFromQueryString(String queryString) {
        final ParsedQueryString parsed = parseQueryStringAndValidateSignature(queryString);

        final Element root = parseSamlMessage(inflate(decodeBase64(parsed.samlRequest)));
        if (REQUEST_TAG_NAME.equals(root.getLocalName()) && SAML_NAMESPACE.equals(root.getNamespaceURI())) {
            try {
                final LogoutRequest logoutRequest = buildXmlObject(root, LogoutRequest.class);
                return parseLogout(logoutRequest, parsed.hasSignature == false, parsed.relayState);
            } catch (ElasticsearchSecurityException e) {
                logger.trace("Rejecting SAML logout request {} because {}", SamlUtils.toString(root), e.getMessage());
                throw e;
            }
        } else {
            throw samlException("SAML content [{}] should have a root element of Namespace=[{}] Tag=[{}]",
                    root, SAML_NAMESPACE, REQUEST_TAG_NAME);
        }
    }

    private ParsedQueryString parseQueryStringAndValidateSignature(String queryString) {
        final String signatureInput = queryString.replaceAll("&Signature=.*$", "");
        final Map<String, String> parameters = new HashMap<>();
        RestUtils.decodeQueryString(queryString, 0, parameters);
        final String samlRequest = parameters.get("SAMLRequest");
        if (samlRequest == null) {
            throw samlException("Could not parse SAMLRequest from query string: [{}]", queryString);
        }

        final String relayState = parameters.get("RelayState");
        final String signatureAlgorithm = parameters.get("SigAlg");
        final String signature = parameters.get("Signature");
        if (signature == null || signatureAlgorithm == null) {
            return new ParsedQueryString(samlRequest, false, relayState);
        }

        validateSignature(signatureInput, signatureAlgorithm, signature);
        return new ParsedQueryString(samlRequest, true, relayState);
    }

    private Result parseLogout(LogoutRequest logoutRequest, boolean requireSignature, String relayState) {
        final Signature signature = logoutRequest.getSignature();
        if (signature == null) {
            if (requireSignature) {
                throw samlException("Logout request is not signed");
            }
        } else {
            validateSignature(signature);
        }

        checkIssuer(logoutRequest.getIssuer(), logoutRequest);
        checkDestination(logoutRequest);
        validateNotOnOrAfter(logoutRequest.getNotOnOrAfter());

        return new Result(logoutRequest.getID(), SamlNameId.fromXml(getNameID(logoutRequest)), getSessionIndex(logoutRequest), relayState);
    }

    private void validateSignature(String inputString, String signatureAlgorithm, String signature) {
        final byte[] sigBytes = decodeBase64(signature);
        final byte[] inputBytes = inputString.getBytes(StandardCharsets.US_ASCII);
        final String signatureText = Strings.cleanTruncate(signature, 32);
        checkIdpSignature(credential -> {
            if (XMLSigningUtil.verifyWithURI(credential, signatureAlgorithm, sigBytes, inputBytes)) {
                logger.debug(() -> new ParameterizedMessage("SAML Signature [{}] matches credentials [{}] [{}]",
                        signatureText, credential.getEntityId(), credential.getPublicKey()));
                return true;
            } else {
                logger.debug(() -> new ParameterizedMessage("SAML Signature [{}] failed against credentials [{}] [{}]",
                        signatureText, credential.getEntityId(), credential.getPublicKey()));
                return false;
            }
        }, signatureText);
    }

    private byte[] decodeBase64(String content) {
        try {
            return Base64.getDecoder().decode(content.replaceAll("\\s+", ""));
        } catch (IllegalArgumentException e) {
            logger.info("Failed to decode base64 string [{}] - {}", content, e.toString());
            throw samlException("SAML message cannot be Base64 decoded", e);
        }
    }

    private byte[] inflate(byte[] bytes) {
        Inflater inflater = new Inflater(true);
        try (ByteArrayInputStream in = new ByteArrayInputStream(bytes);
             InflaterInputStream inflate = new InflaterInputStream(in, inflater);
             ByteArrayOutputStream out = new ByteArrayOutputStream(bytes.length * 3 / 2)) {
            Streams.copy(inflate, out);
            return out.toByteArray();
        } catch (IOException e) {
            throw samlException("SAML message cannot be inflated", e);
        }
    }

    private NameID getNameID(LogoutRequest logoutRequest) {
        final NameID nameID = logoutRequest.getNameID();
        if (nameID == null) {
            final EncryptedID encryptedID = logoutRequest.getEncryptedID();
            if (encryptedID != null) {
                final SAMLObject samlObject = decrypt(encryptedID);
                if (samlObject instanceof NameID) {
                    return (NameID) samlObject;
                }
            }
        }
        return nameID;
    }

    private SAMLObject decrypt(EncryptedID encrypted) {
        if (decrypter == null) {
            throw samlException("SAML EncryptedID [" + text(encrypted, 32) + "] is encrypted, but no decryption key is available");
        }
        try {
            return decrypter.decrypt(encrypted);
        } catch (DecryptionException e) {
            logger.debug(() -> new ParameterizedMessage("Failed to decrypt SAML EncryptedID [{}] with [{}]",
                    text(encrypted, 512), describe(getSpConfiguration().getEncryptionCredentials())), e);
            throw samlException("Failed to decrypt SAML EncryptedID " + text(encrypted, 32), e);
        }
    }

    private String getSessionIndex(LogoutRequest logoutRequest) {
        return logoutRequest.getSessionIndexes()
                .stream()
                .map(as -> as.getSessionIndex())
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    private void checkDestination(LogoutRequest request) {
        final String url = getSpConfiguration().getLogoutUrl();
        if (url == null) {
            throw samlException("SAML request " + request.getID() + " is for destination " + request.getDestination()
                    + " but this realm is not configured for logout");
        }
        if (url.equals(request.getDestination()) == false) {
            throw samlException("SAML request " + request.getID() + " is for destination " + request.getDestination()
                    + " but this realm uses " + url);
        }
    }

    static class ParsedQueryString {
        final String samlRequest;
        final boolean hasSignature;
        final String relayState;

        ParsedQueryString(String samlRequest, boolean hasSignature, String relayState) {
            this.samlRequest = samlRequest;
            this.hasSignature = hasSignature;
            this.relayState = relayState;
        }
    }

    public static class Result {
        private final String requestId;
        private final SamlNameId nameId;
        private final String session;
        private final String relayState;

        public Result(String requestId, SamlNameId nameId, String session, String relayState) {
            this.requestId = requestId;
            this.nameId = nameId;
            this.session = session;
            this.relayState = relayState;
        }

        public String getRequestId() {
            return requestId;
        }

        public SamlNameId getNameId() {
            return nameId;
        }

        public String getSession() {
            return session;
        }

        public String getRelayState() {
            return relayState;
        }

        @Override
        public String toString() {
            return "SamlLogoutRequestHandler.Result{" +
                    "requestId='" + requestId + '\'' +
                    ", nameId=" + nameId +
                    ", session='" + session + '\'' +
                    ", relayState='" + relayState + '\'' +
                    '}';
        }
    }

}
