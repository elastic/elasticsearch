/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.CharArrays;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Response for the successful creation of an api key
 */
public final class CreateApiKeyResponse extends ActionResponse implements ToXContentObject {

    static ConstructingObjectParser<CreateApiKeyResponse, Void> PARSER = new ConstructingObjectParser<>("create_api_key_response",
            args -> new CreateApiKeyResponse((String) args[0], (String) args[1], new SecureString((String) args[2]),
                    (args[3] == null) ? null : Instant.ofEpochMilli((Long) args[3])));
    static {
        PARSER.declareString(constructorArg(), new ParseField("name"));
        PARSER.declareString(constructorArg(), new ParseField("id"));
        PARSER.declareString(constructorArg(), new ParseField("api_key"));
        PARSER.declareLong(optionalConstructorArg(), new ParseField("expiration"));
    }

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
        this.expiration = (expiration != null) ? Instant.ofEpochMilli(expiration.toEpochMilli()): null;
    }

    public CreateApiKeyResponse(StreamInput in) throws IOException {
        super(in);
        this.name = in.readString();
        this.id = in.readString();
        byte[] bytes = null;
        try {
            bytes = in.readByteArray();
            this.key = new SecureString(CharArrays.utf8BytesToChars(bytes));
        } finally {
            if (bytes != null) {
                Arrays.fill(bytes, (byte) 0);
            }
        }
        this.expiration = in.readOptionalInstant();
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

    @Nullable
    public Instant getExpiration() {
        return expiration;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((expiration == null) ? 0 : expiration.hashCode());
        result = prime * result + Objects.hash(id, name, key);
        return result;
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
        if (expiration == null) {
            if (other.expiration != null)
                return false;
        } else if (!Objects.equals(expiration, other.expiration))
            return false;
        return Objects.equals(id, other.id)
                && Objects.equals(key, other.key)
                && Objects.equals(name, other.name);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(id);
        byte[] bytes = null;
        try {
            bytes = CharArrays.toUtf8Bytes(key.getChars());
            out.writeByteArray(bytes);
        } finally {
            if (bytes != null) {
                Arrays.fill(bytes, (byte) 0);
            }
        }
        out.writeOptionalInstant(expiration);
    }

    public static CreateApiKeyResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field("id", id)
            .field("name", name);
        if (expiration != null) {
            builder.field("expiration", expiration.toEpochMilli());
        }
        byte[] charBytes = CharArrays.toUtf8Bytes(key.getChars());
        try {
            builder.field("api_key").utf8Value(charBytes, 0, charBytes.length);
        } finally {
            Arrays.fill(charBytes, (byte) 0);
        }
        return builder.endObject();
    }

    @Override
    public String toString() {
        return "CreateApiKeyResponse [name=" + name + ", id=" + id + ", expiration=" + expiration + "]";
    }

}
