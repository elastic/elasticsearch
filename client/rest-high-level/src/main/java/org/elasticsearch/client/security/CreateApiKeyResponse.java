/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Response for create API key
 */
public final class CreateApiKeyResponse {

    private final String name;
    private final String id;
    private final SecureString key;
    private final Instant expiration;

    public CreateApiKeyResponse(String name, String id, SecureString key, Instant expiration) {
        this.name = name;
        this.id = id;
        this.key = key;
        // As we do not yet support the nanosecond precision when we serialize to JSON,
        // here creating the 'Instant' of milliseconds precision.
        // This Instant can then be used for date comparison.
        this.expiration = (expiration != null) ? Instant.ofEpochMilli(expiration.toEpochMilli()) : null;
    }

    public String getName() {
        return name;
    }

    public String getId() {
        return id;
    }

    public SecureString getKey() {
        return key;
    }

    public SecureString getEncoded() {
        return new SecureString(encode(id, key).toCharArray());
    }

    @Nullable
    public Instant getExpiration() {
        return expiration;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, key, expiration);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final CreateApiKeyResponse other = (CreateApiKeyResponse) obj;
        return Objects.equals(id, other.id)
            && Objects.equals(key, other.key)
            && Objects.equals(name, other.name)
            && Objects.equals(expiration, other.expiration);
    }

    private static String encode(CharSequence id, CharSequence key) {
        return Base64.getEncoder().encodeToString((id + ":" + key).getBytes(StandardCharsets.UTF_8));
    }

    static final ConstructingObjectParser<CreateApiKeyResponse, Void> PARSER = new ConstructingObjectParser<>(
        "create_api_key_response",
        args -> {
            final String id = (String) args[1];
            final String key = (String) args[2];
            if (args[4] != null && false == args[4].equals(encode(id, key))) {
                throw new IllegalArgumentException("the encoded value does not match id and api_key");
            }
            return new CreateApiKeyResponse(
                (String) args[0],
                id,
                new SecureString(key.toCharArray()),
                (args[3] == null) ? null : Instant.ofEpochMilli((Long) args[3])
            );
        }
    );
    static {
        PARSER.declareString(constructorArg(), new ParseField("name"));
        PARSER.declareString(constructorArg(), new ParseField("id"));
        PARSER.declareString(constructorArg(), new ParseField("api_key"));
        PARSER.declareLong(optionalConstructorArg(), new ParseField("expiration"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("encoded"));
    }

    public static CreateApiKeyResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
