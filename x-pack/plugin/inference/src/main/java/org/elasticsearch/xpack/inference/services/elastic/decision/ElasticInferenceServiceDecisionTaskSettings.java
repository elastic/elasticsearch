/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.decision;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.DecisionQuestions;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.inference.ModelConfigurations.TASK_SETTINGS;
import static org.elasticsearch.xpack.inference.services.elastic.decision.ElasticInferenceServiceDecisionServiceSettings.INFERENCE_API_EIS_DECISION;

/**
 * Task settings for the Elastic Inference Service {@code decision} task type, holding the {@link DecisionQuestions} stored on the
 * inference endpoint. Questions are optional when creating the endpoint, so they can be supplied per request instead. Questions in
 * the {@code task_settings} of a request or an endpoint update replace the stored questions as a whole, as merging individual
 * questions would make it hard to remove one.
 *
 * @param questions The stored questions, empty if none were specified
 */
public record ElasticInferenceServiceDecisionTaskSettings(DecisionQuestions questions) implements TaskSettings {

    public static final String NAME = "elastic_decision_task_settings";

    public static final ElasticInferenceServiceDecisionTaskSettings EMPTY = new ElasticInferenceServiceDecisionTaskSettings(
        DecisionQuestions.EMPTY
    );

    private static final ObjectParser<Builder, Void> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, Void> PERSISTENT_PARSER = createParser(true);

    private static ObjectParser<Builder, Void> createParser(boolean ignoreUnknownFields) {
        var parser = new ObjectParser<Builder, Void>(TASK_SETTINGS, ignoreUnknownFields, Builder::new);
        parser.declareObject(
            Builder::setQuestions,
            (p, c) -> DecisionQuestions.parse(p, ignoreUnknownFields),
            new ParseField(DecisionQuestions.QUESTIONS_FIELD)
        );
        return parser;
    }

    /**
     * Parses the task settings of an inference endpoint. Unknown fields are rejected in the {@link ConfigurationParseContext#REQUEST}
     * context and ignored in the {@link ConfigurationParseContext#PERSISTENT} context, so settings stored by a newer version can still
     * be read.
     */
    public static ElasticInferenceServiceDecisionTaskSettings fromMap(
        @Nullable Map<String, Object> map,
        ConfigurationParseContext context
    ) {
        return parse(map, context == ConfigurationParseContext.PERSISTENT).build(EMPTY);
    }

    private static Builder parse(@Nullable Map<String, Object> map, boolean ignoreUnknownFields) {
        if (map == null || map.isEmpty()) {
            return new Builder();
        }

        var parser = ignoreUnknownFields ? PERSISTENT_PARSER : REQUEST_PARSER;
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            return parser.apply(xParser, null);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, TASK_SETTINGS);
        }
    }

    public ElasticInferenceServiceDecisionTaskSettings {
        Objects.requireNonNull(questions);
    }

    public ElasticInferenceServiceDecisionTaskSettings(StreamInput in) throws IOException {
        this(new DecisionQuestions(in));
    }

    @Override
    public boolean isEmpty() {
        return questions.isEmpty();
    }

    /**
     * Applies the task settings of a request or an endpoint update. Questions that are specified replace the stored questions, otherwise
     * the stored questions are kept.
     */
    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return parse(newSettings, false).build(this);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return INFERENCE_API_EIS_DECISION;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        questions.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (questions.isEmpty() == false) {
            builder.field(DecisionQuestions.QUESTIONS_FIELD, questions);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * Collects the parsed settings. Settings that are not set fall back to the ones of the settings they are applied to.
     */
    private static class Builder {
        private DecisionQuestions questions;

        private void setQuestions(DecisionQuestions questions) {
            this.questions = questions;
        }

        private ElasticInferenceServiceDecisionTaskSettings build(ElasticInferenceServiceDecisionTaskSettings original) {
            return new ElasticInferenceServiceDecisionTaskSettings(Objects.requireNonNullElse(questions, original.questions()));
        }
    }
}
