/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class NlpTaskConfig implements ToXContentObject {

    public static final ParseField VOCAB = new ParseField("vocab");
    public static final ParseField TASK_TYPE = new ParseField("task_type");
    public static final ParseField LOWER_CASE = new ParseField("do_lower_case");
    public static final ParseField WITH_SPECIAL_TOKENS = new ParseField("with_special_tokens");
    public static final ParseField CLASSIFICATION_LABELS = new ParseField("classification_labels");
    public static final ParseField MAX_SEQUENCE_LENGTH = new ParseField("max_sequence_length");

    private static final ObjectParser<NlpTaskConfig.Builder, Void> STRICT_PARSER = createParser(false);
    private static final ObjectParser<NlpTaskConfig.Builder, Void> LENIENT_PARSER = createParser(true);

    private static ObjectParser<NlpTaskConfig.Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<NlpTaskConfig.Builder, Void> parser = new ObjectParser<>("task_config",
            ignoreUnknownFields,
            Builder::new);

        parser.declareStringArray(Builder::setVocabulary, VOCAB);
        parser.declareStringArray(Builder::setClassificationLabels, CLASSIFICATION_LABELS);
        parser.declareString(Builder::setTaskType, TASK_TYPE);
        parser.declareBoolean(Builder::setDoLowerCase, LOWER_CASE);
        parser.declareBoolean(Builder::setWithSpecialTokens, WITH_SPECIAL_TOKENS);
        parser.declareInt(Builder::setMaxSequenceLength, MAX_SEQUENCE_LENGTH);
        return parser;
    }

    public static NlpTaskConfig fromXContent(XContentParser parser, boolean lenient) {
        return lenient ? LENIENT_PARSER.apply(parser, null).build() : STRICT_PARSER.apply(parser, null).build();
    }

    public static String documentId(String model) {
        return model + "_task_config";
    }

    public static final int DEFAULT_MAX_SEQUENCE_LENGTH = 512;

    private final TaskType taskType;
    private final List<String> vocabulary;
    private final boolean doLowerCase;
    private final boolean withSpecialTokens;
    private final List<String> classificationLabels;
    private final Integer maxSequenceLength;

    NlpTaskConfig(TaskType taskType, List<String> vocabulary,
                  boolean doLowerCase, boolean withSpecialTokens,
                  List<String> classificationLabels,
                  Integer maxSequenceLen) {
        this.taskType = taskType;
        this.vocabulary = vocabulary;
        this.doLowerCase = doLowerCase;
        this.withSpecialTokens = withSpecialTokens;
        this.classificationLabels = classificationLabels;
        this.maxSequenceLength = maxSequenceLen == null ? DEFAULT_MAX_SEQUENCE_LENGTH : maxSequenceLen;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public List<String> getClassificationLabels() {
        return classificationLabels;
    }

    public BertTokenizer buildTokenizer() {
        return BertTokenizer.builder(vocabulary)
            .setWithSpecialTokens(withSpecialTokens)
            .setDoLowerCase(doLowerCase).build();
    }

    public boolean isDoLowerCase() {
        return doLowerCase;
    }

    public boolean isWithSpecialTokens() {
        return withSpecialTokens;
    }

    public Integer getMaxSequenceLength() {
        return maxSequenceLength;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TASK_TYPE.getPreferredName(), taskType.toString());
        builder.field(VOCAB.getPreferredName(), vocabulary);
        builder.field(LOWER_CASE.getPreferredName(), doLowerCase);
        builder.field(WITH_SPECIAL_TOKENS.getPreferredName(), withSpecialTokens);
        builder.field(MAX_SEQUENCE_LENGTH.getPreferredName(), maxSequenceLength);
        if (classificationLabels != null && classificationLabels.isEmpty() == false) {
            builder.field(CLASSIFICATION_LABELS.getPreferredName(), classificationLabels);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NlpTaskConfig that = (NlpTaskConfig) o;
        return taskType == that.taskType &&
            doLowerCase == that.doLowerCase &&
            withSpecialTokens == that.withSpecialTokens &&
            Objects.equals(maxSequenceLength, that.maxSequenceLength) &&
            Objects.equals(classificationLabels, that.classificationLabels) &&
            Objects.equals(vocabulary, that.vocabulary);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskType, vocabulary, doLowerCase, withSpecialTokens, classificationLabels, maxSequenceLength);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private TaskType taskType;
        private List<String> vocabulary;
        private List<String> classificationLabels;
        private boolean doLowerCase = false;
        private boolean withSpecialTokens = true;
        private Integer maxSequenceLength;

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

        public Builder setDoLowerCase(boolean doLowerCase) {
            this.doLowerCase = doLowerCase;
            return this;
        }

        public Builder setWithSpecialTokens(boolean withSpecialTokens) {
            this.withSpecialTokens = withSpecialTokens;
            return this;
        }

        public Builder setClassificationLabels(List<String> labels) {
            this.classificationLabels = labels;
            return this;
        }

        public Builder setMaxSequenceLength(Integer maxSequenceLength) {
            this.maxSequenceLength = maxSequenceLength;
            return this;
        }

        public NlpTaskConfig build() {
            return new NlpTaskConfig(taskType, vocabulary, doLowerCase, withSpecialTokens, classificationLabels, maxSequenceLength);
        }
    }
}
