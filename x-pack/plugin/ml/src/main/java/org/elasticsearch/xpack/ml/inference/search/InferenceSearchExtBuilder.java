/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.search;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class InferenceSearchExtBuilder extends SearchExtBuilder {

    public static final String NAME = "ml_inference";

    public static final ParseField MODEL_ID = new ParseField("model_id");
    private static final ParseField INFERENCE_CONFIG = new ParseField("inference_config");
    private static final ParseField FIELD_MAPPINGS = new ParseField("field_mappings");
    private static final ParseField TARGET_FIELD = new ParseField("target_field");

    private static final String DEFAULT_TARGET_FIELD = "ml";

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<InferenceSearchExtBuilder, Void> PARSER = new ConstructingObjectParser<>(
            NAME,
            args -> new InferenceSearchExtBuilder((String) args[0], (List<InferenceConfig>) args[1],
                    (String) args[2], (Map<String, String>) args[3])
    );

    static {
        PARSER.declareString(constructorArg(), MODEL_ID);
        PARSER.declareNamedObjects(constructorArg(), (p, c, n) -> p.namedObject(InferenceConfig.class, n, c), INFERENCE_CONFIG);
        PARSER.declareString(optionalConstructorArg(), TARGET_FIELD);
        PARSER.declareField(optionalConstructorArg(), (p, c) -> p.mapStrings(), FIELD_MAPPINGS, ObjectParser.ValueType.OBJECT);
    }

    public static InferenceSearchExtBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String modelId;
    private final InferenceConfig inferenceConfig;
    private final Map<String, String> fieldMap;
    private final String targetField;

    public InferenceSearchExtBuilder(String modelId, List<InferenceConfig> config,
                                     @Nullable String targetField,
                                     @Nullable Map<String, String> fieldMap) {
        this.modelId = modelId;
        if (config != null) {
            assert config.size() == 1;
            this.inferenceConfig = config.get(0);
        } else {
            this.inferenceConfig = null;
        }
        this.targetField = targetField == null ? DEFAULT_TARGET_FIELD : targetField;
        this.fieldMap = fieldMap == null ? Collections.emptyMap() : fieldMap;
    }

    public InferenceSearchExtBuilder(StreamInput in) throws IOException {
        modelId = in.readString();
        inferenceConfig = in.readOptionalNamedWriteable(InferenceConfig.class);
        targetField = in.readString();
        boolean readMap = in.readBoolean();
        if (readMap) {
            fieldMap = in.readMap(StreamInput::readString, StreamInput::readString);
        } else {
            fieldMap = Collections.emptyMap();
        }
    }

    public String getModelId() {
        return modelId;
    }

    public String getTargetField() {
        return targetField;
    }

    public InferenceConfig getInferenceConfig() {
        return inferenceConfig;
    }

    public Map<String, String> getFieldMap() {
        return fieldMap;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeOptionalNamedWriteable(inferenceConfig);
        out.writeString(targetField);
        boolean fieldMapDefined = fieldMap != null && fieldMap.isEmpty() == false;
        out.writeBoolean(fieldMapDefined);
        if (fieldMapDefined) {
            out.writeMap(fieldMap, StreamOutput::writeString, StreamOutput::writeString);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(MODEL_ID.getPreferredName(), modelId);
        if (inferenceConfig != null) {
            builder.startObject(INFERENCE_CONFIG.getPreferredName());
            builder.field(inferenceConfig.getName(), inferenceConfig);
            builder.endObject();
        }
        if (fieldMap != null) {
            builder.field(FIELD_MAPPINGS.getPreferredName(), fieldMap);
        }

        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, inferenceConfig, fieldMap);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        InferenceSearchExtBuilder other = (InferenceSearchExtBuilder) obj;
        return Objects.equals(modelId, other.modelId)
                && Objects.equals(inferenceConfig, other.inferenceConfig)
                && Objects.equals(fieldMap, other.fieldMap);
    }
}
