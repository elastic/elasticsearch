/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.namedcredentials;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Fetches one or all named credentials with the auth block redacted. A null credential
 * name means "list all".
 */
public final class GetNamedCredentialsAction {

    public static final String NAME = "cluster:admin/xpack/security/named_credentials/get";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    private GetNamedCredentialsAction() {/* no instances */}

    public static final class Request extends UntypedActionRequest {

        @Nullable
        private final String credentialName;

        public Request(@Nullable String credentialName) {
            this.credentialName = credentialName;
        }

        @Nullable
        public String credentialName() {
            return credentialName;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }
    }

    public static final class Response extends ActionResponse implements ToXContentObject {

        private final List<NamedCredential> credentials;
        private final boolean single;

        public Response(List<NamedCredential> credentials, boolean single) {
            if (single && credentials.size() != 1) {
                throw new IllegalArgumentException(
                    "a single-credential response must contain exactly one credential, got " + credentials.size()
                );
            }
            this.credentials = credentials;
            this.single = single;
        }

        public List<NamedCredential> credentials() {
            return credentials;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (single) {
                return credentials.get(0).toXContent(builder, params);
            }
            builder.startObject();
            builder.startArray("named_credentials");
            for (NamedCredential credential : credentials) {
                credential.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }
    }
}
