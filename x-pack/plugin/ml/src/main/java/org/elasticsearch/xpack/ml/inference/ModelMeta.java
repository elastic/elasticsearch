/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.util.Objects;

public class ModelMeta implements ToXContentObject, Writeable {

    private static String MODEL_META = "model_meta";
    private static ParseField MODEL = new ParseField("model");
    private static ParseField TYPE = new ParseField("type");

    public static final ConstructingObjectParser<ModelMeta, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<ModelMeta, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<ModelMeta, Void> createParser(boolean lenient) {
        ConstructingObjectParser<ModelMeta, Void> parser = new ConstructingObjectParser<>("model_meta", lenient,
                a -> new ModelMeta((String) a[0], (String) a[1]));

        parser.declareString(ConstructingObjectParser.constructorArg(),  MODEL);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), TYPE);
        return parser;
    }

    public static ModelMeta fromXContent(XContentParser parser) throws IOException {
        return STRICT_PARSER.parse(parser, null);
    }

    public static String documentId(String modelId) {
        return modelId;
    }

    private final String modelId;
    private final String type;

    public ModelMeta(String modelId, String type) {
        this.modelId = modelId;
        this.type = type;
    }

    public ModelMeta(StreamInput in) throws IOException {
        modelId = in.readString();
        type = in.readString();
    }

    public String getModelId() {
        return modelId;
    }

    public String getType() {
        return type;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(getModelId());
        out.writeString(getType());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ModelMeta other = (ModelMeta) obj;

        return Objects.equals(modelId, other.modelId) && Objects.equals(type, other.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, type);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (params.paramAsBoolean(ToXContentParams.INCLUDE_TYPE, false) == true) {
            builder.field(DatafeedConfig.CONFIG_TYPE.getPreferredName(), MODEL_META);
        }
        builder.field(TYPE.getPreferredName(), getType());
        builder.field(MODEL.getPreferredName(), getModelId());
        return builder.endObject();
    }
}
