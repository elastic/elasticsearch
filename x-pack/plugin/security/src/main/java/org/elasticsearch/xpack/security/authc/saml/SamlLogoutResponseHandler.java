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

    public void handle(String content, Collection<String> allowedSamlRequestIds) {
        final ParsedQueryString parsed = parseQueryStringAndValidateSignature(content, "SAMLResponse");
        final Element root;
        if (parsed.hasSignature) {
            logger.info("Content is signed at URL level. Proceed as HTTP-Redirect binding");
            root = parseSamlMessage(inflate(decodeBase64(parsed.samlMessage)));
        } else {
            logger.info("Content is not signed at URL level. Proceed as HTTP-POST binding");
            root = parseSamlMessage(decodeBase64(parsed.samlMessage));
        }

        if (LOGOUT_RESPONSE_TAG_NAME.equals(root.getLocalName()) && SAML_NAMESPACE.equals(root.getNamespaceURI())) {
            final LogoutResponse logoutResponse = buildXmlObject(root, LogoutResponse.class);
            if (logoutResponse == null) {
                throw samlException("Cannot convert element {} into LogoutResponse object", root);
            }
            if (logoutResponse.getSignature() == null) {
                if (parsed.hasSignature == false) {
                    throw samlException("LogoutResponse is not signed for HTTP-Post binding");
                }
            } else {
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
