/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.common.unit.TimeValue;
import org.opensaml.saml.saml2.core.LogoutResponse;
import org.w3c.dom.Element;

import java.time.Clock;
import java.util.Collection;

import static org.elasticsearch.xpack.security.authc.saml.SamlUtils.samlException;

public class SamlLogoutResponseHandler extends SamlResponseHandler {

    private static final String LOGOUT_RESPONSE_TAG_NAME = "LogoutResponse";

    public SamlLogoutResponseHandler(
        Clock clock, IdpConfiguration idp, SpConfiguration sp, TimeValue maxSkew) {
        super(clock, idp, sp, maxSkew);
    }

    public void handle(boolean httpRedirect, String payload, Collection<String> allowedSamlRequestIds) {
        final ParsedQueryString parsed = parseQueryStringAndValidateSignature(payload, "SAMLResponse");
        if (httpRedirect && parsed.hasSignature == false) {
            throw samlException("URL is not signed, but is required for HTTP-Redirect binding");
        } else if (httpRedirect == false && parsed.hasSignature) {
            throw samlException("URL is signed, but binding is HTTP-POST");
        }

        final Element root;
        if (httpRedirect) {
            logger.info("Process SAML LogoutResponse with HTTP-Redirect binding");
            root = parseSamlMessage(inflate(decodeBase64(parsed.samlMessage)));
        } else {
            logger.info("Process SAML LogoutResponse with HTTP-POST binding");
            root = parseSamlMessage(decodeBase64(parsed.samlMessage));
        }

        if (LOGOUT_RESPONSE_TAG_NAME.equals(root.getLocalName()) && SAML_NAMESPACE.equals(root.getNamespaceURI())) {
            final LogoutResponse logoutResponse = buildXmlObject(root, LogoutResponse.class);
            if (logoutResponse == null) {
                throw samlException("Cannot convert element {} into LogoutResponse object", root);
            }
            // For HTTP-Redirect, we validate the signature while parsing the object from the query string
            if (httpRedirect == false && logoutResponse.getSignature() == null) {
                throw samlException("LogoutResponse is not signed, but a signature is required for HTTP-Post binding");
            } else if (httpRedirect == false) {
                validateSignature(logoutResponse.getSignature());
            }
            checkInResponseTo(logoutResponse, allowedSamlRequestIds);
            checkStatus(logoutResponse.getStatus());
            checkIssuer(logoutResponse.getIssuer(), logoutResponse);
            checkResponseDestination(logoutResponse, getSpConfiguration().getLogoutUrl());
        } else {
            throw samlException("SAML content [{}] should have a root element of Namespace=[{}] Tag=[{}]",
                root, SAML_NAMESPACE, LOGOUT_RESPONSE_TAG_NAME);
        }
    }
}
