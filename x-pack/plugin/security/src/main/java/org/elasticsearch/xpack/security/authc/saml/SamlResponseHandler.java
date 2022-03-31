/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.opensaml.saml.saml2.core.Status;
import org.opensaml.saml.saml2.core.StatusCode;
import org.opensaml.saml.saml2.core.StatusDetail;
import org.opensaml.saml.saml2.core.StatusMessage;
import org.opensaml.saml.saml2.core.StatusResponseType;

import java.time.Clock;
import java.util.Collection;

import static org.elasticsearch.xpack.security.authc.saml.SamlUtils.samlException;

public class SamlResponseHandler extends SamlObjectHandler {
    public SamlResponseHandler(Clock clock, IdpConfiguration idp, SpConfiguration sp, TimeValue maxSkew) {
        super(clock, idp, sp, maxSkew);
    }

    protected void checkInResponseTo(StatusResponseType response, Collection<String> allowedSamlRequestIds) {
        if (Strings.hasText(response.getInResponseTo()) && allowedSamlRequestIds.contains(response.getInResponseTo()) == false) {
            logger.debug(
                "The SAML Response with ID [{}] is unsolicited. A user might have used a stale URL or the Identity Provider "
                    + "incorrectly populates the InResponseTo attribute",
                response.getID()
            );
            throw samlException(
                "SAML content is in-response-to [{}] but expected one of {} ",
                response.getInResponseTo(),
                allowedSamlRequestIds
            );
        }
    }

    protected String getStatusCodeMessage(Status status) {
        StatusCode firstLevel = status.getStatusCode();
        StatusCode subLevel = firstLevel.getStatusCode();
        StringBuilder sb = new StringBuilder();
        if (StatusCode.REQUESTER.equals(firstLevel.getValue())) {
            sb.append("The SAML IdP did not grant the request. It indicated that the Elastic Stack side sent something invalid (");
        } else if (StatusCode.RESPONDER.equals(firstLevel.getValue())) {
            sb.append("The request could not be granted due to an error in the SAML IDP side (");
        } else if (StatusCode.VERSION_MISMATCH.equals(firstLevel.getValue())) {
            sb.append("The request could not be granted because the SAML IDP doesn't support SAML 2.0 (");
        } else {
            sb.append("The request could not be granted, the SAML IDP responded with a non-standard Status code (");
        }
        sb.append(firstLevel.getValue()).append(").");
        if (getMessage(status) != null) {
            sb.append(" Message: [").append(getMessage(status)).append("]");
        }
        if (getDetail(status) != null) {
            sb.append(" Detail: [").append(getDetail(status)).append("]");
        }
        if (null != subLevel) {
            sb.append(" Specific status code which might indicate what the issue is: [").append(subLevel.getValue()).append("]");
        }
        return sb.toString();
    }

    protected void checkResponseDestination(StatusResponseType response, String spConfiguredUrl) {
        if (spConfiguredUrl.equals(response.getDestination()) == false) {
            if (response.isSigned() || Strings.hasText(response.getDestination())) {
                throw samlException(
                    "SAML response "
                        + response.getID()
                        + " is for destination "
                        + response.getDestination()
                        + " but this realm uses "
                        + spConfiguredUrl
                );
            }
        }
    }

    protected void checkStatus(Status status) {
        if (status == null || status.getStatusCode() == null) {
            throw samlException("SAML Response has no status code");
        }
        if (isSuccess(status) == false) {
            throw samlException("SAML Response is not a 'success' response: {}", getStatusCodeMessage(status));
        }
    }

    protected boolean isSuccess(Status status) {
        return StatusCode.SUCCESS.equals(status.getStatusCode().getValue());
    }

    private String getMessage(Status status) {
        final StatusMessage sm = status.getStatusMessage();
        return sm == null ? null : sm.getValue();
    }

    private String getDetail(Status status) {
        final StatusDetail sd = status.getStatusDetail();
        return sd == null ? null : SamlUtils.toString(sd.getDOM());
    }
}
