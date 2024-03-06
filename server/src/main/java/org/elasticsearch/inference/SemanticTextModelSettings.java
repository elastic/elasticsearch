/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Serialization class for specifying the settings of a model from semantic_text inference to field mapper.
 * See {@link org.elasticsearch.action.bulk.BulkShardRequestInferenceProvider}
 */
public class SemanticTextModelSettings {

    public static final String NAME = "model_settings";
    public static final ParseField TASK_TYPE_FIELD = new ParseField("task_type");
    public static final ParseField INFERENCE_ID_FIELD = new ParseField("inference_id");
    public static final ParseField DIMENSIONS_FIELD = new ParseField("dimensions");
    public static final ParseField SIMILARITY_FIELD = new ParseField("similarity");
    private final TaskType taskType;
    private final String inferenceId;
    private final Integer dimensions;
    private final SimilarityMeasure similarity;

    public SemanticTextModelSettings(TaskType taskType, String inferenceId, Integer dimensions, SimilarityMeasure similarity) {
        Objects.requireNonNull(taskType, "task type must not be null");
        Objects.requireNonNull(inferenceId, "inferenceId must not be null");
        this.taskType = taskType;
        this.inferenceId = inferenceId;
        this.dimensions = dimensions;
        this.similarity = similarity;
    }

    public SemanticTextModelSettings(Model model) {
        this(
            model.getTaskType(),
            model.getInferenceEntityId(),
            model.getServiceSettings().dimensions(),
            model.getServiceSettings().similarity()
        );
    }

    public static SemanticTextModelSettings parse(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    private static final ConstructingObjectParser<SemanticTextModelSettings, Void> PARSER = new ConstructingObjectParser<>(NAME, args -> {
        TaskType taskType = TaskType.fromString((String) args[0]);
        String inferenceId = (String) args[1];
        Integer dimensions = (Integer) args[2];
        SimilarityMeasure similarity = args[3] == null ? null : SimilarityMeasure.fromString((String) args[3]);
        return new SemanticTextModelSettings(taskType, inferenceId, dimensions, similarity);
    });
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TASK_TYPE_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INFERENCE_ID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), DIMENSIONS_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), SIMILARITY_FIELD);
    }

    public Map<String, Object> asMap() {
        Map<String, Object> attrsMap = new HashMap<>();
        attrsMap.put(TASK_TYPE_FIELD.getPreferredName(), taskType.toString());
        attrsMap.put(INFERENCE_ID_FIELD.getPreferredName(), inferenceId);
        if (dimensions != null) {
            attrsMap.put(DIMENSIONS_FIELD.getPreferredName(), dimensions);
        }
        if (similarity != null) {
            attrsMap.put(SIMILARITY_FIELD.getPreferredName(), similarity);
        }
        return Map.of(NAME, attrsMap);
    }

    public TaskType taskType() {
        return taskType;
    }

    public String inferenceId() {
        return inferenceId;
    }

    public Integer dimensions() {
        return dimensions;
    }

    public SimilarityMeasure similarity() {
        return similarity;
    }
}
