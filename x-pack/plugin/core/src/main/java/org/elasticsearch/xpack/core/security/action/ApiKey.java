/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * API key information
 */
public final class ApiKey implements ToXContentObject, Writeable {

    private final String name;
    private final String id;
    private final Instant creation;
    private final Instant expiration;
    private final boolean invalidated;
    private final String username;
    private final String realm;

    public ApiKey(String name, String id, Instant creation, Instant expiration, boolean invalidated, String username, String realm) {
        this.name = name;
        this.id = id;
        // As we do not yet support the nanosecond precision when we serialize to JSON,
        // here creating the 'Instant' of milliseconds precision.
        // This Instant can then be used for date comparison.
        this.creation = Instant.ofEpochMilli(creation.toEpochMilli());
        this.expiration = (expiration != null) ? Instant.ofEpochMilli(expiration.toEpochMilli()): null;
        this.invalidated = invalidated;
        this.username = username;
        this.realm = realm;
    }

    public ApiKey(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_7_5_0)) {
            this.name = in.readOptionalString();
        } else {
            this.name = in.readString();
        }
        this.id = in.readString();
        this.creation = in.readInstant();
        this.expiration = in.readOptionalInstant();
        this.invalidated = in.readBoolean();
        this.username = in.readString();
        this.realm = in.readString();
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Instant getCreation() {
        return creation;
    }

    public Instant getExpiration() {
        return expiration;
    }

    public boolean isInvalidated() {
        return invalidated;
    }

    public String getUsername() {
        return username;
    }

    public String getRealm() {
        return realm;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
        .field("id", id)
        .field("name", name)
        .field("creation", creation.toEpochMilli());
        if (expiration != null) {
            builder.field("expiration", expiration.toEpochMilli());
        }
        builder.field("invalidated", invalidated)
        .field("username", username)
        .field("realm", realm);
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_7_5_0)) {
            out.writeOptionalString(name);
        } else {
            out.writeString(name);
        }
        out.writeString(id);
        out.writeInstant(creation);
        out.writeOptionalInstant(expiration);
        out.writeBoolean(invalidated);
        out.writeString(username);
        out.writeString(realm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, id, creation, expiration, invalidated, username, realm);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ApiKey other = (ApiKey) obj;
        return Objects.equals(name, other.name)
                && Objects.equals(id, other.id)
                && Objects.equals(creation, other.creation)
                && Objects.equals(expiration, other.expiration)
                && Objects.equals(invalidated, other.invalidated)
                && Objects.equals(username, other.username)
                && Objects.equals(realm, other.realm);
    }

    static final ConstructingObjectParser<ApiKey, Void> PARSER = new ConstructingObjectParser<>("api_key", args -> {
        return new ApiKey((String) args[0], (String) args[1], Instant.ofEpochMilli((Long) args[2]),
                (args[3] == null) ? null : Instant.ofEpochMilli((Long) args[3]), (Boolean) args[4], (String) args[5], (String) args[6]);
    });
    static {
        PARSER.declareString(constructorArg(), new ParseField("name"));
        PARSER.declareString(constructorArg(), new ParseField("id"));
        PARSER.declareLong(constructorArg(), new ParseField("creation"));
        PARSER.declareLong(optionalConstructorArg(), new ParseField("expiration"));
        PARSER.declareBoolean(constructorArg(), new ParseField("invalidated"));
        PARSER.declareString(constructorArg(), new ParseField("username"));
        PARSER.declareString(constructorArg(), new ParseField("realm"));
    }

    public static ApiKey fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String toString() {
        return "ApiKey [name=" + name + ", id=" + id + ", creation=" + creation + ", expiration=" + expiration + ", invalidated="
                + invalidated + ", username=" + username + ", realm=" + realm + "]";
    }

}
