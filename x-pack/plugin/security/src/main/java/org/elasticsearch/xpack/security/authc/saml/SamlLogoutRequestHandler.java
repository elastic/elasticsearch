/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.opensaml.saml.common.SAMLObject;
import org.opensaml.saml.saml2.core.EncryptedID;
import org.opensaml.saml.saml2.core.LogoutRequest;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.xmlsec.encryption.support.DecryptionException;
import org.opensaml.xmlsec.signature.Signature;
import org.w3c.dom.Element;

import java.time.Clock;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.security.authc.saml.SamlUtils.samlException;

/**
 * Processes a LogoutRequest for an IdP-initiated logout.
 */
public class SamlLogoutRequestHandler extends SamlObjectHandler {

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
        final ParsedQueryString parsed = parseQueryStringAndValidateSignature(queryString, "SAMLRequest");

        final Element root = parseSamlMessage(inflate(decodeBase64(parsed.samlMessage)));
        if (REQUEST_TAG_NAME.equals(root.getLocalName()) && SAML_NAMESPACE.equals(root.getNamespaceURI())) {
            try {
                final LogoutRequest logoutRequest = buildXmlObject(root, LogoutRequest.class);
                return parseLogout(logoutRequest, parsed.hasSignature == false, parsed.relayState);
            } catch (ElasticsearchSecurityException e) {
                logger.trace("Rejecting SAML logout request {} because {}", SamlUtils.toString(root), e.getMessage());
                throw e;
            }
        } else {
            throw samlException(
                "SAML content [{}] should have a root element of Namespace=[{}] Tag=[{}]",
                root,
                SAML_NAMESPACE,
                REQUEST_TAG_NAME
            );
        }
    }

    private Result parseLogout(LogoutRequest logoutRequest, boolean requireSignature, String relayState) {
        final Signature signature = logoutRequest.getSignature();
        if (signature == null) {
            if (requireSignature) {
                throw samlException("Logout request is not signed");
            }
        } else {
            validateSignature(signature, logoutRequest.getIssuer());
        }

        checkIssuer(logoutRequest.getIssuer(), logoutRequest);
        checkDestination(logoutRequest);
        validateNotOnOrAfter(logoutRequest.getNotOnOrAfter());

        return new Result(logoutRequest.getID(), SamlNameId.fromXml(getNameID(logoutRequest)), getSessionIndex(logoutRequest), relayState);
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
            logger.debug(
                () -> format(
                    "Failed to decrypt SAML EncryptedID [%s] with [%s]",
                    text(encrypted, 512),
                    describe(getSpConfiguration().getEncryptionCredentials())
                ),
                e
            );
            throw samlException("Failed to decrypt SAML EncryptedID " + text(encrypted, 32), e);
        }
    }

    private static String getSessionIndex(LogoutRequest logoutRequest) {
        return logoutRequest.getSessionIndexes().stream().map(as -> as.getValue()).filter(Objects::nonNull).findFirst().orElse(null);
    }

    private void checkDestination(LogoutRequest request) {
        final String url = getSpConfiguration().getLogoutUrl();
        if (url == null) {
            throw samlException(
                "SAML request "
                    + request.getID()
                    + " is for destination "
                    + request.getDestination()
                    + " but this realm is not configured for logout"
            );
        }
        if (url.equals(request.getDestination()) == false) {
            throw samlException(
                "SAML request " + request.getID() + " is for destination " + request.getDestination() + " but this realm uses " + url
            );
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
            return "SamlLogoutRequestHandler.Result{"
                + "requestId='"
                + requestId
                + '\''
                + ", nameId="
                + nameId
                + ", session='"
                + session
                + '\''
                + ", relayState='"
                + relayState
                + '\''
                + '}';
        }
    }

}
