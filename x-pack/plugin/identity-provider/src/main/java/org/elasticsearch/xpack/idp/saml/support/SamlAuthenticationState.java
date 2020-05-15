/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.saml.support;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents a state object that can be used during an SP initiated flow in order to pass necessary parameters from the response of the
 * /validate API to the /init API. State properties are not integrity protected, might end up in a cookie on the user's side and as such
 * are treated as tainted parameters for informational purposes only. No authentication/authorization decisions should be made based on
 * these.
 */
public class SamlAuthenticationState implements Writeable, ToXContentObject {
    private String entityId;
    private String requestedAcsUrl;
    @Nullable
    private String requestedNameidFormat;
    @Nullable
    private String authnRequestId;

    public SamlAuthenticationState() {

    }

    public SamlAuthenticationState(StreamInput in) throws IOException {
        entityId = in.readString();
        requestedAcsUrl = in.readString();
        requestedNameidFormat = in.readOptionalString();
        authnRequestId = in.readOptionalString();
    }

    public String getRequestedNameidFormat() {
        return requestedNameidFormat;
    }

    public void setRequestedNameidFormat(String requestedNameidFormat) {
        this.requestedNameidFormat = requestedNameidFormat;
    }

    public String getAuthnRequestId() {
        return authnRequestId;
    }

    public void setAuthnRequestId(String authnRequestId) {
        this.authnRequestId = authnRequestId;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public String getRequestedAcsUrl() {
        return requestedAcsUrl;
    }

    public void setRequestedAcsUrl(String requestedAcsUrl) {
        this.requestedAcsUrl = requestedAcsUrl;
    }

    public ValidationException validate() {
        final ValidationException validation = new ValidationException();
        if (Strings.isNullOrEmpty(entityId)) {
            validation.addValidationError("field [" + Fields.ENTITY_ID + "] is required, but was [" + entityId + "]");
        }
        if (Strings.isNullOrEmpty(requestedAcsUrl)) {
            validation.addValidationError("field [" + Fields.ACS_URL + "] is required, but was [" + requestedAcsUrl + "]");
        }
        return validation;
    }

    public static final ObjectParser<SamlAuthenticationState, SamlAuthenticationState> PARSER
        = new ObjectParser<>("saml_authn_state", true, SamlAuthenticationState::new);

    static {
        PARSER.declareString(SamlAuthenticationState::setEntityId, Fields.ENTITY_ID);
        PARSER.declareString(SamlAuthenticationState::setRequestedAcsUrl, Fields.ACS_URL);
        PARSER.declareStringOrNull(SamlAuthenticationState::setRequestedNameidFormat, Fields.NAMEID_FORMAT);
        PARSER.declareStringOrNull(SamlAuthenticationState::setAuthnRequestId, Fields.AUTHN_REQUEST_ID);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Fields.NAMEID_FORMAT.getPreferredName(), requestedNameidFormat);
        builder.field(Fields.AUTHN_REQUEST_ID.getPreferredName(), authnRequestId);
        builder.field(Fields.ENTITY_ID.getPreferredName(), entityId);
        builder.field(Fields.ACS_URL.getPreferredName(), requestedAcsUrl);
        return builder.endObject();
    }

    public static SamlAuthenticationState fromXContent(XContentParser parser) throws IOException {
        SamlAuthenticationState authnState = new SamlAuthenticationState();
        return PARSER.parse(parser, authnState, null);
    }

    public interface Fields {
        ParseField NAMEID_FORMAT = new ParseField("nameid_format");
        ParseField AUTHN_REQUEST_ID = new ParseField("authn_request_id");
        ParseField ENTITY_ID = new ParseField("entity_id");
        ParseField ACS_URL = new ParseField("acs_url");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(entityId);
        out.writeString(requestedAcsUrl);
        out.writeOptionalString(requestedNameidFormat);
        out.writeOptionalString(authnRequestId);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SamlAuthenticationState that = (SamlAuthenticationState) o;
        return entityId.equals(that.entityId) &&
            requestedAcsUrl.equals(that.requestedAcsUrl) &&
            Objects.equals(requestedNameidFormat, that.requestedNameidFormat) &&
            Objects.equals(authnRequestId, that.authnRequestId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityId, requestedAcsUrl, requestedNameidFormat, authnRequestId);
    }
}
