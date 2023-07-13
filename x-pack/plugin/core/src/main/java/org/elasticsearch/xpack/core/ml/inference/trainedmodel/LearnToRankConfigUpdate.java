/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.LearnToRankFeatureExtractorBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfig.FEATURE_EXTRACTORS;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfig.NUM_TOP_FEATURE_IMPORTANCE_VALUES;

public class LearnToRankConfigUpdate implements InferenceConfigUpdate, NamedXContentObject {

    public static final ParseField NAME = LearnToRankConfig.NAME;

    public static LearnToRankConfigUpdate EMPTY_PARAMS = new LearnToRankConfigUpdate(null, null);

    public static LearnToRankConfigUpdate fromConfig(LearnToRankConfig config) {
        return new LearnToRankConfigUpdate(config.getNumTopFeatureImportanceValues(), config.getFeatureExtractorBuilders());
    }

    private static final ObjectParser<LearnToRankConfigUpdate.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<LearnToRankConfigUpdate.Builder, Void> createParser(boolean lenient) {
        ObjectParser<LearnToRankConfigUpdate.Builder, Void> parser = new ObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            LearnToRankConfigUpdate.Builder::new
        );
        parser.declareInt(LearnToRankConfigUpdate.Builder::setNumTopFeatureImportanceValues, NUM_TOP_FEATURE_IMPORTANCE_VALUES);
        parser.declareNamedObjects(
            LearnToRankConfigUpdate.Builder::setFeatureExtractorBuilders,
            (p, c, n) -> p.namedObject(LearnToRankFeatureExtractorBuilder.class, n, false),
            b -> {},
            FEATURE_EXTRACTORS
        );
        return parser;
    }

    public static LearnToRankConfigUpdate fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    private final Integer numTopFeatureImportanceValues;
    private final List<LearnToRankFeatureExtractorBuilder> featureExtractorBuilderList;

    public LearnToRankConfigUpdate(
        Integer numTopFeatureImportanceValues,
        List<LearnToRankFeatureExtractorBuilder> featureExtractorBuilders
    ) {
        if (numTopFeatureImportanceValues != null && numTopFeatureImportanceValues < 0) {
            throw new IllegalArgumentException(
                "[" + NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName() + "] must be greater than or equal to 0"
            );
        }
        if (featureExtractorBuilders != null) {
            Set<String> featureNames = featureExtractorBuilders.stream()
                .map(LearnToRankFeatureExtractorBuilder::featureName)
                .collect(Collectors.toSet());
            if (featureNames.size() < featureExtractorBuilders.size()) {
                throw new IllegalArgumentException(
                    "[" + FEATURE_EXTRACTORS.getPreferredName() + "] contains duplicate [feature_name] values"
                );
            }
        }
        this.numTopFeatureImportanceValues = numTopFeatureImportanceValues;
        this.featureExtractorBuilderList = featureExtractorBuilders == null ? List.of() : featureExtractorBuilders;
    }

    public LearnToRankConfigUpdate(StreamInput in) throws IOException {
        this.numTopFeatureImportanceValues = in.readOptionalVInt();
        this.featureExtractorBuilderList = in.readNamedWriteableList(LearnToRankFeatureExtractorBuilder.class);
    }

    public Integer getNumTopFeatureImportanceValues() {
        return numTopFeatureImportanceValues;
    }

    @Override
    public String getResultsField() {
        return DEFAULT_RESULTS_FIELD;
    }

    @Override
    public InferenceConfigUpdate.Builder<? extends InferenceConfigUpdate.Builder<?, ?>, ? extends InferenceConfigUpdate> newBuilder() {
        return new Builder().setNumTopFeatureImportanceValues(numTopFeatureImportanceValues);
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalVInt(numTopFeatureImportanceValues);
        out.writeNamedWriteableList(featureExtractorBuilderList);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return LearnToRankConfig.MIN_SUPPORTED_TRANSPORT_VERSION;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (numTopFeatureImportanceValues != null) {
            builder.field(NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName(), numTopFeatureImportanceValues);
        }
        if (featureExtractorBuilderList.isEmpty() == false) {
            NamedXContentObjectHelper.writeNamedObjects(
                builder,
                params,
                true,
                FEATURE_EXTRACTORS.getPreferredName(),
                featureExtractorBuilderList
            );
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LearnToRankConfigUpdate that = (LearnToRankConfigUpdate) o;
        return Objects.equals(this.numTopFeatureImportanceValues, that.numTopFeatureImportanceValues)
            && Objects.equals(this.featureExtractorBuilderList, that.featureExtractorBuilderList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numTopFeatureImportanceValues, featureExtractorBuilderList);
    }

    @Override
    public LearnToRankConfig apply(InferenceConfig originalConfig) {
        if (originalConfig instanceof LearnToRankConfig == false) {
            throw ExceptionsHelper.badRequestException(
                "Inference config of type [{}] can not be updated with a inference request of type [{}]",
                originalConfig.getName(),
                getName()
            );
        }

        LearnToRankConfig ltrConfig = (LearnToRankConfig) originalConfig;
        if (isNoop(ltrConfig)) {
            return ltrConfig;
        }
        LearnToRankConfig.Builder builder = new LearnToRankConfig.Builder(ltrConfig);
        if (numTopFeatureImportanceValues != null) {
            builder.setNumTopFeatureImportanceValues(numTopFeatureImportanceValues);
        }
        if (featureExtractorBuilderList.isEmpty() == false) {
            Map<String, LearnToRankFeatureExtractorBuilder> existingExtractors = ltrConfig.getFeatureExtractorBuilders()
                .stream()
                .collect(Collectors.toMap(LearnToRankFeatureExtractorBuilder::featureName, f -> f));
            featureExtractorBuilderList.forEach(f -> existingExtractors.put(f.featureName(), f));
            builder.setLearnToRankFeatureExtractorBuilders(new ArrayList<>(existingExtractors.values()));
        }
        return builder.build();
    }

    @Override
    public boolean isSupported(InferenceConfig inferenceConfig) {
        return inferenceConfig instanceof LearnToRankConfig;
    }

    boolean isNoop(LearnToRankConfig originalConfig) {
        return (numTopFeatureImportanceValues == null || originalConfig.getNumTopFeatureImportanceValues() == numTopFeatureImportanceValues)
            && (featureExtractorBuilderList.isEmpty()
                || Objects.equals(originalConfig.getFeatureExtractorBuilders(), featureExtractorBuilderList));
    }

    public static class Builder implements InferenceConfigUpdate.Builder<Builder, LearnToRankConfigUpdate> {
        private Integer numTopFeatureImportanceValues;
        private List<LearnToRankFeatureExtractorBuilder> featureExtractorBuilderList;

        @Override
        public Builder setResultsField(String resultsField) {
            assert false : "results field should never be set in ltr config";
            return this;
        }

        public Builder setNumTopFeatureImportanceValues(Integer numTopFeatureImportanceValues) {
            this.numTopFeatureImportanceValues = numTopFeatureImportanceValues;
            return this;
        }

        public Builder setFeatureExtractorBuilders(List<LearnToRankFeatureExtractorBuilder> featureExtractorBuilderList) {
            this.featureExtractorBuilderList = featureExtractorBuilderList;
            return this;
        }

        @Override
        public LearnToRankConfigUpdate build() {
            return new LearnToRankConfigUpdate(numTopFeatureImportanceValues, featureExtractorBuilderList);
        }
    }
}
