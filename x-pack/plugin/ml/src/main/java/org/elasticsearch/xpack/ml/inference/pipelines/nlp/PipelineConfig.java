/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pipelines.nlp;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.inference.pipelines.nlp.tokenizers.BertTokenizer;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

public class PipelineConfig implements ToXContentObject {

    public static final ParseField VOCAB = new ParseField("vocab");
    public static final ParseField TASK_TYPE = new ParseField("task_type");

    private static final ObjectParser<PipelineConfig.Builder, Void> STRICT_PARSER = createParser(false);
    private static final ObjectParser<PipelineConfig.Builder, Void> LENIENT_PARSER = createParser(true);

    private static ObjectParser<PipelineConfig.Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<PipelineConfig.Builder, Void> parser = new ObjectParser<>("pipeline_config",
            ignoreUnknownFields,
            Builder::new);

        parser.declareStringArray(Builder::setVocabulary, VOCAB);
        parser.declareString(Builder::setTaskType, TASK_TYPE);
        return parser;
    }

    public static PipelineConfig fromXContent(XContentParser parser, boolean lenient) {
        return lenient ? LENIENT_PARSER.apply(parser, null).build() : STRICT_PARSER.apply(parser, null).build();
    }

    public static String documentId(String model) {
        return model + "_pipeline_config";
    }

    private final TaskType taskType;
    private final List<String> vocabulary;

    PipelineConfig(TaskType taskType, List<String> vocabulary) {
        this.taskType = taskType;
        this.vocabulary = vocabulary;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public BertTokenizer buildTokenizer() {
        return BertTokenizer.builder(vocabMap()).build();
    }

    SortedMap<String, Integer> vocabMap() {
        SortedMap<String, Integer> vocab = new TreeMap<>();
        for (int i = 0; i < vocabulary.size(); i++) {
            vocab.put(vocabulary.get(i), i);
        }
        return vocab;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TASK_TYPE.getPreferredName(), taskType.toString());
        builder.field(VOCAB.getPreferredName(), vocabulary);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PipelineConfig that = (PipelineConfig) o;
        return taskType == that.taskType && Objects.equals(vocabulary, that.vocabulary);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskType, vocabulary);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private TaskType taskType;
        private List<String> vocabulary;

        public Builder setTaskType(TaskType taskType) {
            this.taskType = taskType;
            return this;
        }

        public Builder setTaskType(String taskType) {
            this.taskType = TaskType.fromString(taskType);
            return this;
        }

        public Builder setVocabulary(List<String> vocab) {
            this.vocabulary = vocab;
            return this;
        }

        public PipelineConfig build() {
            return new PipelineConfig(taskType, vocabulary);
        }
    }
}
