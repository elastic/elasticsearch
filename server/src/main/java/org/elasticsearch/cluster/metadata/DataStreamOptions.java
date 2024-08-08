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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Holds data stream dedicated configuration options such as failure store, (in the future lifecycle). Currently, it
 * supports the following configurations:
 * - lifecycle
 * - failure store
 */
public record DataStreamOptions(@Nullable DataStreamLifecycle lifecycle, @Nullable DataStreamFailureStore failureStore)
    implements
        SimpleDiffable<DataStreamOptions>,
        ToXContentObject {

    public static final ParseField LIFECYCLE_FIELD = new ParseField("lifecycle");
    public static final ParseField FAILURE_STORE_FIELD = new ParseField("failure_store");

    public static final ConstructingObjectParser<DataStreamOptions, Void> PARSER = new ConstructingObjectParser<>(
        "options",
        false,
        (args, unused) -> new DataStreamOptions((DataStreamLifecycle) args[0], (DataStreamFailureStore) args[1])
    );

    static {
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> DataStreamLifecycle.fromXContent(p),
            LIFECYCLE_FIELD,
            ObjectParser.ValueType.OBJECT_OR_NULL
        );
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> DataStreamFailureStore.fromXContent(p),
            FAILURE_STORE_FIELD,
            ObjectParser.ValueType.OBJECT_OR_NULL
        );
    }

    public DataStreamOptions() {
        this(null, null);
    }

    public DataStreamOptions(StreamInput in) throws IOException {
        this(in.readOptionalWriteable(DataStreamLifecycle::new), in.readOptionalWriteable(DataStreamFailureStore::new));
    }

    public static Diff<DataStreamOptions> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(DataStreamOptions::new, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(lifecycle);
        out.writeOptionalWriteable(failureStore);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (lifecycle != null) {
            builder.field(LIFECYCLE_FIELD.getPreferredName(), lifecycle);
        }
        if (failureStore != null) {
            builder.field(FAILURE_STORE_FIELD.getPreferredName(), failureStore);
        }
        builder.endObject();
        return builder;
    }

    public static DataStreamOptions fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
