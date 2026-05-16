/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.GenericNamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A validated data source setting value paired with its sensitivity classification, stored in
 * cluster state as part of data source metadata.
 *
 * <p>Secret values are either a transient plaintext {@code String} (only on the way into the
 * master-side encrypt step) or a ciphertext {@link GenericNamedWriteable} carrier deposited by
 * that step. After encryption every secret value persisted to cluster state is a carrier; any other
 * shape read back from cluster state is stale pre-encryption data and consumers should fail.
 *
 * <p>Non-secret values may carry any type that round-trips through {@link StreamOutput#writeGenericValue}.
 * Access values via {@link #rawValue()} (always returns the raw value) or {@link #nonSecretValue()}
 * (asserts {@code !secret}).
 */
public final class DataSourceSetting implements Writeable, ToXContentObject {

    static final String MASK_SENTINEL = "::es_redacted::";

    private static final ParseField VALUE = new ParseField("value");
    private static final ParseField SECRET = new ParseField("secret");

    private static final ConstructingObjectParser<DataSourceSetting, Void> PARSER = new ConstructingObjectParser<>(
        "data_source_setting",
        false,
        (args, ctx) -> new DataSourceSetting(args[0], (boolean) args[1])
    );

    static {
        // XContent input arrives only via user PUTs (REST layer) — secrets are scalar plaintext at that
        // boundary. The ciphertext carrier shape is binary-only on the wire and never round-trips
        // through XContent in the API or persistence paths (DataSourceMetadata.context() excludes both).
        PARSER.declareField(ConstructingObjectParser.constructorArg(), (p, c) -> p.objectText(), VALUE, ObjectParser.ValueType.VALUE);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), SECRET);
    }

    private final Object value;
    private final boolean secret;

    public DataSourceSetting(Object value, boolean secret) {
        if (secret && value != null) {
            if ((value instanceof String || value instanceof GenericNamedWriteable) == false) {
                throw new IllegalArgumentException(
                    "secret data source settings must be a String (transient plaintext) or an encrypted carrier; got ["
                        + value.getClass().getName()
                        + "]"
                );
            }
        }
        this.value = value;
        this.secret = secret;
    }

    public DataSourceSetting(StreamInput in) throws IOException {
        this(in.readGenericValue(), in.readBoolean());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericValue(value);
        out.writeBoolean(secret);
    }

    public boolean secret() {
        return secret;
    }

    /** Returns the value of a non-secret setting; throws if secret. */
    public Object nonSecretValue() {
        if (secret) {
            throw new IllegalStateException("secret setting — use rawValue() and route through the decrypt helper");
        }
        return value;
    }

    /** Raw value irrespective of secret flag. Callers must route secret values through the consumer-side decrypt helper. */
    public Object rawValue() {
        return value;
    }

    /** Returns the masked sentinel for secrets, or the plaintext value otherwise. Safe for REST responses. */
    public Object presentationValue() {
        return secret ? MASK_SENTINEL : value;
    }

    public static DataSourceSetting fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (value instanceof ToXContentObject toX) {
            builder.field(VALUE.getPreferredName());
            toX.toXContent(builder, params);
        } else {
            builder.field(VALUE.getPreferredName(), value);
        }
        builder.field(SECRET.getPreferredName(), secret);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataSourceSetting that = (DataSourceSetting) o;
        return secret == that.secret && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, secret);
    }

    @Override
    public String toString() {
        return "DataSourceSetting{value=" + (secret ? MASK_SENTINEL : value) + ", secret=" + secret + "}";
    }
}
