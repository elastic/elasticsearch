/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.search;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class InferenceSearchExtBuilder extends SearchExtBuilder {

    public static final String NAME = "model";

    public static final ParseField MODEL_ID = new ParseField("model_id");
    private static final ParseField INFERENCE_CONFIG = new ParseField("inference_config");
    private static final ParseField FIELD_MAPPINGS = new ParseField("field_mappings");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<InferenceSearchExtBuilder, Void> PARSER = new ConstructingObjectParser<>(
            NAME,
            args -> new InferenceSearchExtBuilder((String) args[0], (List<InferenceConfig>) args[1], (Map<String, String>) args[2])
    );

    static {
        PARSER.declareString(constructorArg(), MODEL_ID);
        PARSER.declareNamedObjects(optionalConstructorArg(), (p, c, n) -> p.namedObject(InferenceConfig.class, n, c), INFERENCE_CONFIG);
        PARSER.declareField(optionalConstructorArg(), (p, c) -> p.mapStrings(), FIELD_MAPPINGS, ObjectParser.ValueType.OBJECT);
    }

    private final String modelId;
    private final InferenceConfig inferenceConfig;
    private final Map<String, String> fieldMap;

    public InferenceSearchExtBuilder(String modelId, @Nullable List<InferenceConfig> config, @Nullable Map<String, String> fieldMap) {
        this.modelId = modelId;
        if (config != null) {
            assert config.size() == 1;
            this.inferenceConfig = config.get(0);
        } else {
            this.inferenceConfig = null;
        }
        this.fieldMap = fieldMap;
    }

    @Override
    public String getWriteableName() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }
}
