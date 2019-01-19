/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

/**
 * API key information
 */
public final class ApiKeyInfo implements ToXContentObject, Writeable, Streamable {

    private final String name;
    private final String id;
    private final Instant creation;
    private final Instant expiration;
    private final boolean invalidated;
    private final String username;
    private final String realm;

    public ApiKeyInfo(String name, String id, Instant creation, Instant expiration, boolean invalidated, String username, String realm) {
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

    public ApiKeyInfo(StreamInput in) throws IOException {
        this.name = in.readString();
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
        .field("creation", creation.toEpochMilli())
        .field("invalidated", invalidated)
        .field("username", username)
        .field("realm", realm);
        if (expiration != null) {
            builder.field("expiration", expiration.toEpochMilli());
        }
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(id);
        out.writeInstant(creation);
        out.writeOptionalInstant(expiration);
        out.writeBoolean(invalidated);
        out.writeString(username);
        out.writeString(realm);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
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
        ApiKeyInfo other = (ApiKeyInfo) obj;
        return Objects.equals(name, other.name)
                && Objects.equals(id, other.id)
                && Objects.equals(creation, other.creation)
                && Objects.equals(expiration, other.expiration)
                && Objects.equals(invalidated, other.invalidated)
                && Objects.equals(username, other.username)
                && Objects.equals(realm, other.realm);
    }

    @Override
    public String toString() {
        return "ApiKeyInfo [name=" + name + ", id=" + id + ", creation=" + creation + ", expiration=" + expiration + ", invalidated="
                + invalidated + ", username=" + username + ", realm=" + realm + "]";
    }

}
