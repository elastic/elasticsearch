/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.inference;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class TrainedModelLocation implements ToXContentObject {

    public static final String NAME = "model_location";
    public static final ParseField MODEL_ID = new ParseField("model_id");
    public static final ParseField INDEX = new ParseField("index");

    private static final ConstructingObjectParser<TrainedModelLocation, Void> PARSER =
        new ConstructingObjectParser<>(NAME, true, a -> new TrainedModelLocation((String) a[0], (String) a[1] ));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), MODEL_ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX);
    }

    public static TrainedModelLocation fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final String modelId;
    private final String index;

    public TrainedModelLocation(String modelId, String index) {
        this.modelId = Objects.requireNonNull(modelId);
        this.index = Objects.requireNonNull(index);
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
