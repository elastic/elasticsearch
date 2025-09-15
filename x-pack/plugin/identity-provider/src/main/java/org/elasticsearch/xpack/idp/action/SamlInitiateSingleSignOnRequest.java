/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.idp.saml.support.SamlAuthenticationState;
import org.elasticsearch.xpack.idp.saml.support.SamlInitiateSingleSignOnAttributes;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class SamlInitiateSingleSignOnRequest extends LegacyActionRequest {

    private String spEntityId;
    private String assertionConsumerService;
    private SamlAuthenticationState samlAuthenticationState;
    private SamlInitiateSingleSignOnAttributes attributes;

    public SamlInitiateSingleSignOnRequest(StreamInput in) throws IOException {
        super(in);
        spEntityId = in.readString();
        assertionConsumerService = in.readString();
        samlAuthenticationState = in.readOptionalWriteable(SamlAuthenticationState::new);
        if (in.getTransportVersion().onOrAfter(TransportVersions.IDP_CUSTOM_SAML_ATTRIBUTES_ADDED_8_19)) {
            attributes = in.readOptionalWriteable(SamlInitiateSingleSignOnAttributes::new);
        }
    }

    public SamlInitiateSingleSignOnRequest() {}

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isNullOrEmpty(spEntityId)) {
            validationException = addValidationError("entity_id is missing", validationException);
        }
        if (Strings.isNullOrEmpty(assertionConsumerService)) {
            validationException = addValidationError("acs is missing", validationException);
        }

        // Validate attributes if present
        if (attributes != null) {
            ActionRequestValidationException attributesValidationException = attributes.validate();
            if (attributesValidationException != null) {
                for (String error : attributesValidationException.validationErrors()) {
                    validationException = addValidationError(error, validationException);
                }
            }
        }

        return validationException;
    }

    public String getSpEntityId() {
        return spEntityId;
    }

    public void setSpEntityId(String spEntityId) {
        this.spEntityId = spEntityId;
    }

    public String getAssertionConsumerService() {
        return assertionConsumerService;
    }

    public void setAssertionConsumerService(String assertionConsumerService) {
        this.assertionConsumerService = assertionConsumerService;
    }

    public SamlAuthenticationState getSamlAuthenticationState() {
        return samlAuthenticationState;
    }

    public void setSamlAuthenticationState(SamlAuthenticationState samlAuthenticationState) {
        this.samlAuthenticationState = samlAuthenticationState;
    }

    public SamlInitiateSingleSignOnAttributes getAttributes() {
        return attributes;
    }

    public void setAttributes(SamlInitiateSingleSignOnAttributes attributes) {
        this.attributes = attributes;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(spEntityId);
        out.writeString(assertionConsumerService);
        out.writeOptionalWriteable(samlAuthenticationState);
        if (out.getTransportVersion().onOrAfter(TransportVersions.IDP_CUSTOM_SAML_ATTRIBUTES_ADDED_8_19)) {
            out.writeOptionalWriteable(attributes);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "{"
            + "spEntityId='"
            + spEntityId
            + "', "
            + "acs='"
            + assertionConsumerService
            + "', "
            + "attributes='"
            + attributes
            + "'}";
    }

}
