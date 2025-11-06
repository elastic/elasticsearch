/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutCCMConfigurationAction extends ActionType<CCMEnabledActionResponse> {

    public static final PutCCMConfigurationAction INSTANCE = new PutCCMConfigurationAction();
    public static final String NAME = "cluster:admin/xpack/inference/ccm/put";

    public PutCCMConfigurationAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        private static final ObjectParser<Request.Builder, Void> PARSER = new ObjectParser<>(NAME, Request.Builder::new);

        static {
            PARSER.declareString(Request.Builder::setApiKey, new ParseField("api_key"));
            PARSER.declareBoolean(Request.Builder::setEnabled, new ParseField("enabled"));
        }

        public static Request parseRequest(TimeValue masterNodeTimeout, TimeValue ackTimeout, XContentParser parser) throws IOException {
            var builder = PARSER.parse(parser, null);
            return builder.build(masterNodeTimeout, ackTimeout);
        }

        public static Request createEnabled(SecureString apiKey, TimeValue masterNodeTimeout, TimeValue ackTimeout) {
            return new Request(Objects.requireNonNull(apiKey), null, masterNodeTimeout, ackTimeout);
        }

        public static Request createDisabled(TimeValue masterNodeTimeout, TimeValue ackTimeout) {
            return new Request(null, Boolean.FALSE, masterNodeTimeout, ackTimeout);
        }

        private final SecureString apiKey;
        private final Boolean enabled;

        public Request(SecureString apiKey, Boolean enabled, TimeValue masterNodeTimeout, TimeValue ackTimeout) {
            super(masterNodeTimeout, ackTimeout);
            this.apiKey = apiKey;
            this.enabled = enabled;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            apiKey = in.readOptionalSecureString();
            enabled = in.readOptionalBoolean();
        }

        public SecureString getApiKey() {
            return apiKey;
        }

        public Boolean getEnabledField() {
            return enabled;
        }

        @Override
        public ActionRequestValidationException validate() {
            if (apiKey == null && enabled == null) {
                return addValidationError("At least one of [api_key] or [enabled] must be provided", null);
            }

            if (apiKey != null && enabled != null) {
                return addValidationError("Only one of [api_key] or [enabled] can be provided but not both", null);
            }

            if (enabled != null && enabled) {
                return addValidationError(
                    "The [enabled] field must be set to [false] when disabling CCM, "
                        + "otherwise omit it and provide the [api_key] field instead",
                    null
                );
            }

            if (apiKey != null && Strings.isEmpty(apiKey.toString())) {
                return addValidationError("The [api_key] field cannot be an empty string", null);
            }

            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalSecureString(apiKey);
            out.writeOptionalBoolean(enabled);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(apiKey, request.apiKey) && Objects.equals(enabled, request.enabled);
        }

        @Override
        public int hashCode() {
            return Objects.hash(apiKey, enabled);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (apiKey != null) {
                builder.field("api_key", apiKey.toString());
            }

            if (enabled != null) {
                builder.field("enabled", enabled);
            }
            builder.endObject();
            return builder;
        }

        public static class Builder {
            private SecureString apiKey;
            private Boolean enabled;

            private Builder() {}

            public Builder setApiKey(SecureString apiKey) {
                this.apiKey = apiKey;
                return this;
            }

            public Builder setApiKey(String apiKey) {
                this.apiKey = new SecureString(apiKey.toCharArray());
                return this;
            }

            public Builder setEnabled(Boolean enabled) {
                this.enabled = enabled;
                return this;
            }

            public Request build(TimeValue masterNodeTimeout, TimeValue ackTimeout) {
                return new Request(apiKey, enabled, masterNodeTimeout, ackTimeout);
            }
        }
    }
}
