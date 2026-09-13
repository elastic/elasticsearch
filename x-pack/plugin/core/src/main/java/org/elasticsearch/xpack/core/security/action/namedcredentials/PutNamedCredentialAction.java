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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Creates or replaces a named credential (strict full-replace semantics). Secrets in the
 * {@code auth} block are required and are encrypted before storage. Use PATCH for partial updates.
 */
public final class PutNamedCredentialAction {

    public static final String NAME = "cluster:admin/xpack/security/named_credentials/put";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    private PutNamedCredentialAction() {/* no instances */}

    public static final class Request extends UntypedActionRequest {

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Request, String> PARSER = new ConstructingObjectParser<>(
            "put_named_credential_request",
            false,
            (args, name) -> new Request(
                name,
                CredentialAuthType.fromTypeName((String) args[0]),
                (String) args[1],
                (Map<String, String>) args[2],
                (Map<String, String>) args[3]
            )
        );

        static {
            PARSER.declareString(constructorArg(), new ParseField("auth_type"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("url"));
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.mapStrings(), new ParseField("config"));
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.mapStrings(), new ParseField("auth"));
        }

        private final String credentialName;
        private final CredentialAuthType authType;
        @Nullable
        private final String url;
        private final Map<String, String> config;
        @Nullable
        private final Map<String, String> auth;

        public Request(
            String credentialName,
            CredentialAuthType authType,
            @Nullable String url,
            @Nullable Map<String, String> config,
            @Nullable Map<String, String> auth
        ) {
            this.credentialName = credentialName;
            this.authType = authType;
            this.url = url;
            this.config = config == null ? Map.of() : Map.copyOf(config);
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

        public CredentialAuthType authType() {
            return authType;
        }

        @Nullable
        public String url() {
            return url;
        }

        public Map<String, String> config() {
            return config;
        }

        @Nullable
        public Map<String, String> auth() {
            return auth;
        }

        /** Shared name rules: lowercase, index-name-like charset, max 256 chars. */
        public static void validateCredentialName(String name, List<String> errors) {
            if (Strings.isNullOrEmpty(name)) {
                errors.add("credential name must not be empty");
                return;
            }
            if (name.length() > 256) {
                errors.add("credential name must not exceed 256 characters");
            }
            if (name.startsWith("_") || name.startsWith("-")) {
                errors.add("credential name must not start with '_' or '-'");
            }
            boolean validChars = name.chars()
                .allMatch(c -> (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.');
            if (validChars == false) {
                errors.add("credential name must contain only lowercase letters, digits, '.', '_', and '-'");
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            List<String> errors = new ArrayList<>();
            validateCredentialName(credentialName, errors);
            if (auth == null) {
                errors.add("auth is required for PUT; use PATCH to update individual fields");
            } else if (auth.isEmpty()) {
                errors.add("auth must not be empty");
            } else {
                errors.addAll(authType.validateAuth(auth));
            }
            errors.addAll(authType.validateConfig(config));
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

    /** Response for PUT; {@code created} is {@code true} when the credential was newly created, {@code false} when replaced. */
    public static final class Response extends ActionResponse implements ToXContentObject {

        private final boolean created;

        public Response(boolean created) {
            this.created = created;
        }

        public boolean created() {
            return created;
        }

        public RestStatus status() {
            return created ? RestStatus.CREATED : RestStatus.OK;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("acknowledged", true);
            builder.field("created", created);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }
    }
}
