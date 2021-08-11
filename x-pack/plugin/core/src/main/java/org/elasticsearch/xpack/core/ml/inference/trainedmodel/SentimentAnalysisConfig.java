/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class SentimentAnalysisConfig implements NlpConfig {

    public static final String NAME = "sentiment_analysis";

    public static SentimentAnalysisConfig fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null);
    }

    public static SentimentAnalysisConfig fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null);
    }

    private static final ConstructingObjectParser<SentimentAnalysisConfig, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<SentimentAnalysisConfig, Void> LENIENT_PARSER = createParser(true);

    @SuppressWarnings({ "unchecked"})
    private static ConstructingObjectParser<SentimentAnalysisConfig, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<SentimentAnalysisConfig, Void> parser = new ConstructingObjectParser<>(NAME, ignoreUnknownFields,
            a -> new SentimentAnalysisConfig((VocabularyConfig) a[0], (TokenizationParams) a[1], (List<String>) a[2]));
        parser.declareObject(ConstructingObjectParser.constructorArg(), VocabularyConfig.createParser(ignoreUnknownFields), VOCABULARY);
        parser.declareObject(ConstructingObjectParser.optionalConstructorArg(), TokenizationParams.createParser(ignoreUnknownFields),
            TOKENIZATION_PARAMS);
        parser.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), CLASSIFICATION_LABELS);
        return parser;
    }

    private final VocabularyConfig vocabularyConfig;
    private final TokenizationParams tokenizationParams;
    private final List<String> classificationLabels;

    public SentimentAnalysisConfig(VocabularyConfig vocabularyConfig, @Nullable TokenizationParams tokenizationParams,
                                   @Nullable List<String> classificationLabels) {
        this.vocabularyConfig = ExceptionsHelper.requireNonNull(vocabularyConfig, VOCABULARY);
        this.tokenizationParams = tokenizationParams == null ? TokenizationParams.createDefault() : tokenizationParams;
        this.classificationLabels = classificationLabels;
    }

    public SentimentAnalysisConfig(StreamInput in) throws IOException {
        vocabularyConfig = new VocabularyConfig(in);
        tokenizationParams = new TokenizationParams(in);
        classificationLabels = in.readOptionalStringList();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(VOCABULARY.getPreferredName(), vocabularyConfig);
        builder.field(TOKENIZATION_PARAMS.getPreferredName(), tokenizationParams);
        if (classificationLabels != null && classificationLabels.isEmpty() == false) {
            builder.field(CLASSIFICATION_LABELS.getPreferredName(), classificationLabels);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        vocabularyConfig.writeTo(out);
        tokenizationParams.writeTo(out);
        out.writeOptionalStringCollection(classificationLabels);
    }

    @Override
    public boolean isTargetTypeSupported(TargetType targetType) {
        return false;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_0_0;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SentimentAnalysisConfig that = (SentimentAnalysisConfig) o;
        return Objects.equals(vocabularyConfig, that.vocabularyConfig)
            && Objects.equals(tokenizationParams, that.tokenizationParams)
            && Objects.equals(classificationLabels, that.classificationLabels);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vocabularyConfig, tokenizationParams, classificationLabels);
    }

    @Override
    public VocabularyConfig getVocabularyConfig() {
        return vocabularyConfig;
    }

    @Override
    public TokenizationParams getTokenizationParams() {
        return tokenizationParams;
    }

    @Nullable
    public List<String> getClassificationLabels() {
        return classificationLabels;
    }
}
