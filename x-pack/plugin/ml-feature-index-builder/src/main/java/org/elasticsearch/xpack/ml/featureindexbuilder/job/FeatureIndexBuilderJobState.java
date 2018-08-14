/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.job;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.indexing.IndexerState;

import java.io.IOException;
import java.util.Objects;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class FeatureIndexBuilderJobState implements Task.Status, PersistentTaskState {
    public static final String NAME = "xpack/feature_index_builder/job";

    private final IndexerState state;

    private static final ParseField STATE = new ParseField("job_state");

    public static final ConstructingObjectParser<FeatureIndexBuilderJobState, Void> PARSER = new ConstructingObjectParser<>(NAME,
            args -> new FeatureIndexBuilderJobState((IndexerState) args[0]));

    static {
        PARSER.declareField(constructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return IndexerState.fromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, STATE, ObjectParser.ValueType.STRING);
    }

    public FeatureIndexBuilderJobState(IndexerState state) {
        this.state = state;
    }

    public FeatureIndexBuilderJobState(StreamInput in) throws IOException {
        state = IndexerState.fromStream(in);
    }

    public IndexerState getJobState() {
        return state;
    }

    public static FeatureIndexBuilderJobState fromXContent(XContentParser parser) {
        try {
            return PARSER.parse(parser, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(STATE.getPreferredName(), state.value());
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        state.writeTo(out);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        FeatureIndexBuilderJobState that = (FeatureIndexBuilderJobState) other;

        return Objects.equals(this.state, that.state);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state);
    }
}