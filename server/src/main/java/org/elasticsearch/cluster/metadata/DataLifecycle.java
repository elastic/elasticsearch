/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Holds the Data Lifecycle Management metadata that are configuring how a data stream is managed.
 */
public class DataLifecycle implements SimpleDiffable<DataLifecycle>, ToXContentFragment {

    public static final DataLifecycle EMPTY = new DataLifecycle();

    private static final ParseField DATA_RETENTION_FIELD = new ParseField("data_retention");

    private static final ConstructingObjectParser<DataLifecycle, Void> PARSER = new ConstructingObjectParser<>(
        "data_lifecycle_metadata_parser",
        false,
        (args, unused) -> new DataLifecycle((TimeValue) args[0])
    );

    static {
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.textOrNull(), DATA_RETENTION_FIELD.getPreferredName()),
            DATA_RETENTION_FIELD,
            ObjectParser.ValueType.STRING_OR_NULL
        );
    }

    @Nullable
    private final TimeValue dataRetention;

    public DataLifecycle() {
        this.dataRetention = null;
    }

    public DataLifecycle(@Nullable TimeValue dataRetention) {
        this.dataRetention = dataRetention;
    }

    @Nullable
    public TimeValue getDataRetention() {
        return dataRetention;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final DataLifecycle that = (DataLifecycle) o;
        return Objects.equals(dataRetention, that.dataRetention);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dataRetention);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalTimeValue(dataRetention);
    }

    public DataLifecycle(StreamInput in) throws IOException {
        dataRetention = in.readOptionalTimeValue();
    }

    public static Diff<DataLifecycle> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(DataLifecycle::new, in);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (dataRetention != null) {
            builder.field(DATA_RETENTION_FIELD.getPreferredName(), dataRetention.getStringRep());
        }
        return builder;
    }

    public static DataLifecycle fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
