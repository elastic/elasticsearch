/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp.saml.support;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
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
    @Nullable
    private String requestedNameidFormat;
    @Nullable
    private String authnRequestId;

    public SamlAuthenticationState() {

    }

    public SamlAuthenticationState(StreamInput in) throws IOException {
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

    public static final ObjectParser<SamlAuthenticationState, SamlAuthenticationState> PARSER
        = new ObjectParser<>("saml_authn_state", true, SamlAuthenticationState::new);

    static {
        PARSER.declareStringOrNull(SamlAuthenticationState::setRequestedNameidFormat, Fields.NAMEID_FORMAT);
        PARSER.declareStringOrNull(SamlAuthenticationState::setAuthnRequestId, Fields.AUTHN_REQUEST_ID);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Fields.NAMEID_FORMAT.getPreferredName(), requestedNameidFormat);
        builder.field(Fields.AUTHN_REQUEST_ID.getPreferredName(), authnRequestId);
        return builder.endObject();
    }

    public static SamlAuthenticationState fromXContent(XContentParser parser) throws IOException {
        SamlAuthenticationState authnState = new SamlAuthenticationState();
        return PARSER.parse(parser, authnState, null);
    }

    public interface Fields {
        ParseField NAMEID_FORMAT = new ParseField("nameid_format");
        ParseField AUTHN_REQUEST_ID = new ParseField("authn_request_id");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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
        return Objects.equals(requestedNameidFormat, that.requestedNameidFormat) &&
            Objects.equals(authnRequestId, that.authnRequestId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestedNameidFormat, authnRequestId);
    }
}
