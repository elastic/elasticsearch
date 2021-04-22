/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class TrainedModelLocation implements ToXContentObject, Writeable {

    public static final String NAME = "model_location";
    public static final ParseField MODEL_ID = new ParseField("model_id");
    public static final ParseField INDEX = new ParseField("index");

    private static final ConstructingObjectParser<TrainedModelLocation, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<TrainedModelLocation, Void> STRICT_PARSER = createParser(false);

    private static ConstructingObjectParser<TrainedModelLocation, Void> createParser(boolean lenient) {
        ConstructingObjectParser<TrainedModelLocation, Void> parser = new ConstructingObjectParser<>(
            NAME,
            lenient,
            a -> new TrainedModelLocation((String) a[0], (String) a[1] ));
        parser.declareString(ConstructingObjectParser.constructorArg(), MODEL_ID);
        parser.declareString(ConstructingObjectParser.constructorArg(), INDEX);
        return parser;
    }

    public static TrainedModelLocation fromXContent(XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.parse(parser, null) : STRICT_PARSER.parse(parser, null);
    }

    private final String modelId;
    private final String index;

    TrainedModelLocation(String modelId, String index) {
        this.modelId = Objects.requireNonNull(modelId);
        this.index = Objects.requireNonNull(index);
    }

    public TrainedModelLocation(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.index = in.readString();
    }

    public String getModelId() {
        return modelId;
    }

    public String getIndex() {
        return index;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_ID.getPreferredName(), modelId);
        builder.field(INDEX.getPreferredName(), index);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeString(index);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TrainedModelLocation that = (TrainedModelLocation) o;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, index);
    }
}
