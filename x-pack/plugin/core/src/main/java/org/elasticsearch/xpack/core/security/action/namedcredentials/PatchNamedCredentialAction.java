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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Partially updates an existing named credential. Only the fields present in the request body
 * are updated; absent fields are carried forward from the stored document.
 * All fields are optional, but at least one must be provided.
 */
public final class PatchNamedCredentialAction {

    public static final String NAME = "cluster:admin/xpack/security/named_credentials/patch";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    private PatchNamedCredentialAction() {/* no instances */}

    public static final class Request extends UntypedActionRequest {

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Request, String> PARSER = new ConstructingObjectParser<>(
            "patch_named_credential_request",
            false,
            (args, name) -> {
                final String authTypeStr = (String) args[0];
                final CredentialAuthType authType = authTypeStr != null ? CredentialAuthType.fromTypeName(authTypeStr) : null;
                return new Request(name, authType, (String) args[1], (Map<String, String>) args[2], (Map<String, String>) args[3]);
            }
        );

        static {
            PARSER.declareString(optionalConstructorArg(), new ParseField("auth_type"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("url"));
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.mapStrings(), new ParseField("config"));
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.mapStrings(), new ParseField("auth"));
        }

        private final String credentialName;
        @Nullable
        private final CredentialAuthType authType;
        @Nullable
        private final String url;
        @Nullable
        private final Map<String, String> config;
        @Nullable
        private final Map<String, String> auth;

        public Request(
            String credentialName,
            @Nullable CredentialAuthType authType,
            @Nullable String url,
            @Nullable Map<String, String> config,
            @Nullable Map<String, String> auth
        ) {
            this.credentialName = credentialName;
            this.authType = authType;
            this.url = url;
            this.config = config == null ? null : Map.copyOf(config);
            this.auth = auth == null ? null : Map.copyOf(auth);
        }

        public static Request fromXContent(String name, XContentParser parser) throws IOException {
            try {
                return PARSER.parse(parser, name);
            } catch (IllegalArgumentException e) {
                // Unwrap any wrapped cause so callers see the root message (e.g. unknown auth_type)
                Throwable cause = e.getCause();
                if (cause instanceof IllegalArgumentException iae) {
                    throw iae;
                }
                throw e;
            }
        }

        public String credentialName() {
            return credentialName;
        }

        @Nullable
        public CredentialAuthType authType() {
            return authType;
        }

        @Nullable
        public String url() {
            return url;
        }

        @Nullable
        public Map<String, String> config() {
            return config;
        }

        @Nullable
        public Map<String, String> auth() {
            return auth;
        }

        @Override
        public ActionRequestValidationException validate() {
            List<String> errors = new ArrayList<>();
            PutNamedCredentialAction.Request.validateCredentialName(credentialName, errors);
            if (authType == null && url == null && config == null && auth == null) {
                errors.add("at least one field must be provided for a PATCH");
            }
            if (auth != null) {
                if (auth.isEmpty()) {
                    errors.add("auth must not be empty when provided");
                } else if (authType != null) {
                    // We know the authType from the request — validate now.
                    errors.addAll(authType.validateAuth(auth));
                }
                // If authType is absent we don't know the stored type; service does the validation.
            }
            if (config != null && authType != null) {
                errors.addAll(authType.validateConfig(config));
            }
            ActionRequestValidationException validationException = null;
            for (String error : errors) {
                validationException = ValidateActions.addValidationError(error, validationException);
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }
    }

    /** Response for PATCH; always {@code acknowledged: true}. */
    public static final class Response extends ActionResponse implements ToXContentObject {

        public Response() {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("acknowledged", true);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }
    }
}
