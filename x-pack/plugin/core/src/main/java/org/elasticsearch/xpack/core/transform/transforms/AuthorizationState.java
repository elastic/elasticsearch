/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;

/**
 * {@link AuthorizationState} holds the state of the authorization performed in the past.
 * By examining the instance of this class the caller can learn whether or not the user was authorized to access the source/dest indices
 * present in the {@link TransformConfig}.
 *
 * This class is immutable.
 */
public class AuthorizationState implements Writeable, ToXContentObject {

    public static AuthorizationState green() {
        return new AuthorizationState(System.currentTimeMillis(), HealthStatus.GREEN, null);
    }

    public static boolean isNullOrGreen(AuthorizationState authState) {
        return authState == null || HealthStatus.GREEN.equals(authState.getStatus());
    }

    public static AuthorizationState red(Exception e) {
        return new AuthorizationState(System.currentTimeMillis(), HealthStatus.RED, e != null ? e.getMessage() : "unknown exception");
    }

    public static final ParseField TIMESTAMP = new ParseField("timestamp");
    public static final ParseField STATUS = new ParseField("status");
    public static final ParseField LAST_AUTH_ERROR = new ParseField("last_auth_error");

    public static final ConstructingObjectParser<AuthorizationState, Void> PARSER = new ConstructingObjectParser<>(
        "transform_authorization_state",
        true,
        a -> new AuthorizationState((Long) a[0], (HealthStatus) a[1], (String) a[2])
    );

    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TIMESTAMP);
        PARSER.declareField(ConstructingObjectParser.constructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return HealthStatus.valueOf(p.text().toUpperCase(Locale.ROOT));
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, STATUS, ObjectParser.ValueType.STRING);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), LAST_AUTH_ERROR);
    }

    private final long timestampMillis;
    private final HealthStatus status;
    @Nullable
    private final String lastAuthError;

    public AuthorizationState(Long timestamp, HealthStatus status, @Nullable String lastAuthError) {
        this.timestampMillis = timestamp;
        this.status = status;
        this.lastAuthError = lastAuthError;
    }

    public AuthorizationState(StreamInput in) throws IOException {
        this.timestampMillis = in.readLong();
        this.status = in.readEnum(HealthStatus.class);
        this.lastAuthError = in.readOptionalString();
    }

    public Instant getTimestamp() {
        return Instant.ofEpochMilli(timestampMillis);
    }

    public HealthStatus getStatus() {
        return status;
    }

    public String getLastAuthError() {
        return lastAuthError;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(TIMESTAMP.getPreferredName(), timestampMillis);
        builder.field(STATUS.getPreferredName(), status.xContentValue());
        if (lastAuthError != null) {
            builder.field(LAST_AUTH_ERROR.getPreferredName(), lastAuthError);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(timestampMillis);
        status.writeTo(out);
        out.writeOptionalString(lastAuthError);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        AuthorizationState that = (AuthorizationState) other;

        return this.timestampMillis == that.timestampMillis
            && this.status.value() == that.status.value()
            && Objects.equals(this.lastAuthError, that.lastAuthError);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestampMillis, status, lastAuthError);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
