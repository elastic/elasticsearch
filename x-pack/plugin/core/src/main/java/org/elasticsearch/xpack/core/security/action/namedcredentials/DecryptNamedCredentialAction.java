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
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Fetches a single named credential with the auth block decrypted to plaintext. This is the
 * only read path that returns secrets; it is gated on the manage_named_credentials privilege.
 */
public final class DecryptNamedCredentialAction {

    public static final String NAME = "cluster:admin/xpack/security/named_credentials/decrypt";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    private DecryptNamedCredentialAction() {/* no instances */}

    public static final class Request extends UntypedActionRequest {

        private final String credentialName;

        public Request(String credentialName) {
            this.credentialName = credentialName;
        }

        public String credentialName() {
            return credentialName;
        }

        @Override
        public ActionRequestValidationException validate() {
            if (Strings.isNullOrEmpty(credentialName)) {
                return ValidateActions.addValidationError("credential name must not be empty", null);
            }
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }
    }

    public static final class Response extends ActionResponse implements ToXContentObject {

        private final NamedCredential credential;

        public Response(NamedCredential credential) {
            assert credential.auth() != null : "decrypt response must carry plaintext auth";
            this.credential = credential;
        }

        public NamedCredential credential() {
            return credential;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return credential.toXContent(builder, params);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }
    }
}
