/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.MapXContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.inference.TaskType.SPARSE_EMBEDDING;
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;

/**
 * Serialization class for specifying the settings of a model from semantic_text inference to field mapper.
 */
public class SemanticTextModelSettings implements ToXContentObject {

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
        validate();
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

    public static SemanticTextModelSettings fromMap(Object node) {
        if (node == null) {
            return null;
        }
        try {
            Map<String, Object> map = XContentMapValues.nodeMapValue(node, NAME);
            if (map.containsKey(INFERENCE_ID_FIELD.getPreferredName()) == false) {
                throw new IllegalArgumentException(
                    "Failed to parse [" + NAME + "], required [" + INFERENCE_ID_FIELD.getPreferredName() + "] is missing"
                );
            }
            if (map.containsKey(TASK_TYPE_FIELD.getPreferredName()) == false) {
                throw new IllegalArgumentException(
                    "Failed to parse [" + NAME + "], required [" + TASK_TYPE_FIELD.getPreferredName() + "] is missing"
                );
            }
            XContentParser parser = new MapXContentParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                map,
                XContentType.JSON
            );
            return SemanticTextModelSettings.parse(parser);
        } catch (Exception exc) {
            throw new ElasticsearchException(exc);
        }
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TASK_TYPE_FIELD.getPreferredName(), taskType.toString());
        builder.field(INFERENCE_ID_FIELD.getPreferredName(), inferenceId);
        if (dimensions != null) {
            builder.field(DIMENSIONS_FIELD.getPreferredName(), dimensions);
        }
        if (similarity != null) {
            builder.field(SIMILARITY_FIELD.getPreferredName(), similarity);
        }
        return builder.endObject();
    }

    public void validate() {
        switch (taskType) {
            case TEXT_EMBEDDING:
            case SPARSE_EMBEDDING:
                break;

            default:
                throw new IllegalArgumentException("Wrong [" + TASK_TYPE_FIELD.getPreferredName() + "], expected " +
                        TEXT_EMBEDDING + "or " + SPARSE_EMBEDDING + ", got " + taskType.name());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SemanticTextModelSettings that = (SemanticTextModelSettings) o;
        return taskType == that.taskType
            && inferenceId.equals(that.inferenceId)
            && Objects.equals(dimensions, that.dimensions)
            && similarity == that.similarity;
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskType, inferenceId, dimensions, similarity);
    }
}
