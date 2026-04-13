/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A validated datasource or dataset setting value paired with its sensitivity classification.
 * This is the representation stored in cluster state as part of datasource metadata.
 *
 * <p>Secret values are masked on GET responses and encrypted when the encryption
 * layer is available. Values preserve their original JSON type (String, Integer,
 * Boolean, etc.) for non-lossy REST API round-trips.
 */
public final class DataSourceStoredSetting implements Writeable, ToXContentObject {

    private static final ParseField VALUE = new ParseField("value");
    private static final ParseField SECRET = new ParseField("secret");

    private static final ConstructingObjectParser<DataSourceStoredSetting, Void> PARSER = new ConstructingObjectParser<>(
        "data_source_stored_setting",
        false,
        (args, ctx) -> new DataSourceStoredSetting(args[0], (boolean) args[1])
    );

    static {
        PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> p.objectText(),
            VALUE,
            org.elasticsearch.xcontent.ObjectParser.ValueType.VALUE
        );
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), SECRET);
    }

    private final Object value;
    private final boolean secret;

    public DataSourceStoredSetting(Object value, boolean secret) {
        // Value type is not validated here — same pattern as MatchQueryBuilder, FuzzyQueryBuilder, and other classes
        // that store an Object payload and serialize via StreamOutput.writeGenericValue. An unsupported type surfaces
        // as an IllegalArgumentException("can not write type [...]") at serialization time.
        this.value = value;
        this.secret = secret;
    }

    public DataSourceStoredSetting(StreamInput in) throws IOException {
        this.value = in.readGenericValue();
        this.secret = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericValue(value);
        out.writeBoolean(secret);
    }

    public Object value() {
        return value;
    }

    public boolean secret() {
        return secret;
    }

    /** Decrypt the value. No-op until encryption ships. */
    public Object decryptedValue() {
        return value;
    }

    /** Returns masked value for API responses, or decrypted value if not secret. */
    public Object maskedOrDecryptedValue() {
        return secret ? "**********" : decryptedValue();
    }

    public static DataSourceStoredSetting fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(VALUE.getPreferredName(), value);
        builder.field(SECRET.getPreferredName(), secret);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataSourceStoredSetting that = (DataSourceStoredSetting) o;
        return secret == that.secret && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, secret);
    }

    @Override
    public String toString() {
        return "DataSourceStoredSetting{value=" + (secret ? "***" : value) + ", secret=" + secret + "}";
    }
}
