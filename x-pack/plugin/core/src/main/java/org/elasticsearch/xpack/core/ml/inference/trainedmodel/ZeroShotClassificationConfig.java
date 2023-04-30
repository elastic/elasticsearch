/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * This builds out a 0-shot classification task.
 *
 * The 0-shot methodology assumed is MNLI optimized task. For further info see: https://arxiv.org/abs/1909.00161
 *
 */
public class ZeroShotClassificationConfig implements NlpConfig {

    public static final String NAME = "zero_shot_classification";
    public static final ParseField HYPOTHESIS_TEMPLATE = new ParseField("hypothesis_template");
    public static final ParseField MULTI_LABEL = new ParseField("multi_label");
    public static final ParseField LABELS = new ParseField("labels");

    public static ZeroShotClassificationConfig fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null);
    }

    public static ZeroShotClassificationConfig fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null);
    }

    private static final Set<String> REQUIRED_CLASSIFICATION_LABELS = new TreeSet<>(List.of("entailment", "neutral", "contradiction"));
    private static final String DEFAULT_HYPOTHESIS_TEMPLATE = "This example is {}.";
    private static final ConstructingObjectParser<ZeroShotClassificationConfig, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<ZeroShotClassificationConfig, Void> LENIENT_PARSER = createParser(true);

    @SuppressWarnings({ "unchecked" })
    private static ConstructingObjectParser<ZeroShotClassificationConfig, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<ZeroShotClassificationConfig, Void> parser = new ConstructingObjectParser<>(
            NAME,
            ignoreUnknownFields,
            a -> new ZeroShotClassificationConfig(
                (List<String>) a[0],
                (VocabularyConfig) a[1],
                (Tokenization) a[2],
                (String) a[3],
                (Boolean) a[4],
                (List<String>) a[5],
                (String) a[6]
            )
        );
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), CLASSIFICATION_LABELS);
        parser.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            if (ignoreUnknownFields == false) {
                throw ExceptionsHelper.badRequestException(
                    "illegal setting [{}] on inference model creation",
                    VOCABULARY.getPreferredName()
                );
            }
            return VocabularyConfig.fromXContentLenient(p);
        }, VOCABULARY);
        parser.declareNamedObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c, n) -> p.namedObject(Tokenization.class, n, ignoreUnknownFields),
            TOKENIZATION
        );
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), HYPOTHESIS_TEMPLATE);
        parser.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), MULTI_LABEL);
        parser.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), LABELS);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), RESULTS_FIELD);
        return parser;
    }

    private final VocabularyConfig vocabularyConfig;
    private final Tokenization tokenization;
    private final List<String> classificationLabels;
    private final List<String> labels;
    private final boolean isMultiLabel;
    private final String hypothesisTemplate;
    private final String resultsField;

    public ZeroShotClassificationConfig(
        List<String> classificationLabels,
        @Nullable VocabularyConfig vocabularyConfig,
        @Nullable Tokenization tokenization,
        @Nullable String hypothesisTemplate,
        @Nullable Boolean isMultiLabel,
        @Nullable List<String> labels,
        @Nullable String resultsField
    ) {
        this.classificationLabels = ExceptionsHelper.requireNonNull(classificationLabels, CLASSIFICATION_LABELS);
        if (this.classificationLabels.size() != 3) {
            throw ExceptionsHelper.badRequestException(
                "[{}] must contain exactly the three values {}",
                CLASSIFICATION_LABELS.getPreferredName(),
                REQUIRED_CLASSIFICATION_LABELS
            );
        }
        List<String> badLabels = classificationLabels.stream()
            .map(s -> s.toLowerCase(Locale.ROOT))
            .filter(c -> REQUIRED_CLASSIFICATION_LABELS.contains(c) == false)
            .collect(Collectors.toList());
        if (badLabels.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException(
                "[{}] must contain exactly the three values {}. Invalid labels {}",
                CLASSIFICATION_LABELS.getPreferredName(),
                REQUIRED_CLASSIFICATION_LABELS,
                badLabels
            );
        }
        this.vocabularyConfig = Optional.ofNullable(vocabularyConfig)
            .orElse(new VocabularyConfig(InferenceIndexConstants.nativeDefinitionStore()));
        this.tokenization = tokenization == null ? Tokenization.createDefault() : tokenization;
        this.isMultiLabel = isMultiLabel != null && isMultiLabel;
        this.hypothesisTemplate = Optional.ofNullable(hypothesisTemplate).orElse(DEFAULT_HYPOTHESIS_TEMPLATE);
        this.labels = labels;
        if (labels != null && labels.isEmpty()) {
            throw ExceptionsHelper.badRequestException("[{}] must not be empty", LABELS.getPreferredName());
        }
        this.resultsField = resultsField;
        if (this.tokenization.span != -1) {
            throw ExceptionsHelper.badRequestException(
                "[{}] does not support windowing long text sequences; configured span [{}]",
                NAME,
                this.tokenization.span
            );
        }
    }

    public ZeroShotClassificationConfig(StreamInput in) throws IOException {
        vocabularyConfig = new VocabularyConfig(in);
        tokenization = in.readNamedWriteable(Tokenization.class);
        classificationLabels = in.readStringList();
        isMultiLabel = in.readBoolean();
        hypothesisTemplate = in.readString();
        labels = in.readOptionalStringList();
        resultsField = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        vocabularyConfig.writeTo(out);
        out.writeNamedWriteable(tokenization);
        out.writeStringCollection(classificationLabels);
        out.writeBoolean(isMultiLabel);
        out.writeString(hypothesisTemplate);
        out.writeOptionalStringCollection(labels);
        out.writeOptionalString(resultsField);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(VOCABULARY.getPreferredName(), vocabularyConfig, params);
        NamedXContentObjectHelper.writeNamedObject(builder, params, TOKENIZATION.getPreferredName(), tokenization);
        builder.field(CLASSIFICATION_LABELS.getPreferredName(), classificationLabels);
        builder.field(MULTI_LABEL.getPreferredName(), isMultiLabel);
        builder.field(HYPOTHESIS_TEMPLATE.getPreferredName(), hypothesisTemplate);
        if (labels != null) {
            builder.field(LABELS.getPreferredName(), labels);
        }
        if (resultsField != null) {
            builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean isTargetTypeSupported(TargetType targetType) {
        return false;
    }

    @Override
    public Version getMinimalSupportedNodeVersion() {
        return Version.V_8_0_0;
    }

    @Override
    public TransportVersion getMinimalSupportedTransportVersion() {
        return TransportVersion.V_8_0_0;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ZeroShotClassificationConfig that = (ZeroShotClassificationConfig) o;
        return Objects.equals(vocabularyConfig, that.vocabularyConfig)
            && Objects.equals(tokenization, that.tokenization)
            && Objects.equals(isMultiLabel, that.isMultiLabel)
            && Objects.equals(hypothesisTemplate, that.hypothesisTemplate)
            && Objects.equals(labels, that.labels)
            && Objects.equals(classificationLabels, that.classificationLabels)
            && Objects.equals(resultsField, that.resultsField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vocabularyConfig, tokenization, classificationLabels, hypothesisTemplate, isMultiLabel, labels, resultsField);
    }

    @Override
    public VocabularyConfig getVocabularyConfig() {
        return vocabularyConfig;
    }

    @Override
    public Tokenization getTokenization() {
        return tokenization;
    }

    public List<String> getClassificationLabels() {
        return classificationLabels;
    }

    public boolean isMultiLabel() {
        return isMultiLabel;
    }

    public String getHypothesisTemplate() {
        return hypothesisTemplate;
    }

    public Optional<List<String>> getLabels() {
        return Optional.ofNullable(labels);
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public boolean isAllocateOnly() {
        return true;
    }

}
