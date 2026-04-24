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
    static final String API_KEY_FIELD_ERROR = "The [api_key] field cannot be an empty string or null";

    public PutCCMConfigurationAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        private static final ObjectParser<Request.Builder, Void> PARSER = new ObjectParser<>(NAME, Request.Builder::new);

        static {
            PARSER.declareString(Request.Builder::setApiKey, new ParseField("api_key"));
        }

        public static Request parseRequest(TimeValue masterNodeTimeout, TimeValue ackTimeout, XContentParser parser) throws IOException {
            var builder = PARSER.parse(parser, null);
            return builder.build(masterNodeTimeout, ackTimeout);
        }

        public static Request createEnabled(String apiKey, TimeValue masterNodeTimeout, TimeValue ackTimeout) {
            return new Request(new SecureString(Objects.requireNonNull(apiKey).toCharArray()), masterNodeTimeout, ackTimeout);
        }

        private final SecureString apiKey;

        public Request(SecureString apiKey, TimeValue masterNodeTimeout, TimeValue ackTimeout) {
            super(masterNodeTimeout, ackTimeout);
            this.apiKey = apiKey;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            apiKey = in.readSecureString();
        }

        public SecureString getApiKey() {
            return apiKey;
        }

        @Override
        public ActionRequestValidationException validate() {
            if (apiKey == null || Strings.isNullOrEmpty(apiKey.toString())) {
                return addValidationError(API_KEY_FIELD_ERROR, null);
            }

            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeSecureString(apiKey);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(apiKey, request.apiKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(apiKey);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (apiKey != null) {
                builder.field("api_key", apiKey.toString());
            }

            builder.endObject();
            return builder;
        }

        public static class Builder {
            private SecureString apiKey;

            private Builder() {}

            public Builder setApiKey(SecureString apiKey) {
                this.apiKey = apiKey;
                return this;
            }

            public Builder setApiKey(String apiKey) {
                this.apiKey = new SecureString(apiKey.toCharArray());
                return this;
            }

            public Request build(TimeValue masterNodeTimeout, TimeValue ackTimeout) {
                return new Request(apiKey, masterNodeTimeout, ackTimeout);
            }
        }
    }
}
