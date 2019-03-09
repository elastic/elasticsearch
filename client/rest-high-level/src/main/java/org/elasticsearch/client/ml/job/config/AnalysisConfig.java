/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Analysis configuration options that describe which fields are
 * analyzed and which functions are used to detect anomalies.
 * <p>
 * The configuration can contain multiple detectors, a new anomaly detector will
 * be created for each detector configuration. The fields
 * <code>bucketSpan, summaryCountFieldName and categorizationFieldName</code>
 * apply to all detectors.
 * <p>
 * If a value has not been set it will be <code>null</code>
 * Object wrappers are used around integral types &amp; booleans so they can take
 * <code>null</code> values.
 */
public class AnalysisConfig implements ToXContentObject {
    /**
     * Serialisation names
     */
    public static final ParseField ANALYSIS_CONFIG = new ParseField("analysis_config");
    public static final ParseField BUCKET_SPAN = new ParseField("bucket_span");
    public static final ParseField CATEGORIZATION_FIELD_NAME = new ParseField("categorization_field_name");
    public static final ParseField CATEGORIZATION_FILTERS = new ParseField("categorization_filters");
    public static final ParseField CATEGORIZATION_ANALYZER = CategorizationAnalyzerConfig.CATEGORIZATION_ANALYZER;
    public static final ParseField LATENCY = new ParseField("latency");
    public static final ParseField SUMMARY_COUNT_FIELD_NAME = new ParseField("summary_count_field_name");
    public static final ParseField DETECTORS = new ParseField("detectors");
    public static final ParseField INFLUENCERS = new ParseField("influencers");
    public static final ParseField MULTIVARIATE_BY_FIELDS = new ParseField("multivariate_by_fields");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<Builder, Void> PARSER = new ConstructingObjectParser<>(ANALYSIS_CONFIG.getPreferredName(),
        true, a -> new AnalysisConfig.Builder((List<Detector>) a[0]));

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(),
            (p, c) -> (Detector.PARSER).apply(p, c).build(), DETECTORS);
        PARSER.declareString((builder, val) ->
            builder.setBucketSpan(TimeValue.parseTimeValue(val, BUCKET_SPAN.getPreferredName())), BUCKET_SPAN);
        PARSER.declareString(Builder::setCategorizationFieldName, CATEGORIZATION_FIELD_NAME);
        PARSER.declareStringArray(Builder::setCategorizationFilters, CATEGORIZATION_FILTERS);
        // This one is nasty - the syntax for analyzers takes either names or objects at many levels, hence it's not
        // possible to simply declare whether the field is a string or object and a completely custom parser is required
        PARSER.declareField(Builder::setCategorizationAnalyzerConfig,
            (p, c) -> CategorizationAnalyzerConfig.buildFromXContentFragment(p),
            CATEGORIZATION_ANALYZER, ObjectParser.ValueType.OBJECT_OR_STRING);
        PARSER.declareString((builder, val) ->
            builder.setLatency(TimeValue.parseTimeValue(val, LATENCY.getPreferredName())), LATENCY);
        PARSER.declareString(Builder::setSummaryCountFieldName, SUMMARY_COUNT_FIELD_NAME);
        PARSER.declareStringArray(Builder::setInfluencers, INFLUENCERS);
        PARSER.declareBoolean(Builder::setMultivariateByFields, MULTIVARIATE_BY_FIELDS);
    }

    /**
     * These values apply to all detectors
     */
    private final TimeValue bucketSpan;
    private final String categorizationFieldName;
    private final List<String> categorizationFilters;
    private final CategorizationAnalyzerConfig categorizationAnalyzerConfig;
    private final TimeValue latency;
    private final String summaryCountFieldName;
    private final List<Detector> detectors;
    private final List<String> influencers;
    private final Boolean multivariateByFields;

    private AnalysisConfig(TimeValue bucketSpan, String categorizationFieldName, List<String> categorizationFilters,
                           CategorizationAnalyzerConfig categorizationAnalyzerConfig, TimeValue latency, String summaryCountFieldName,
                           List<Detector> detectors, List<String> influencers, Boolean multivariateByFields) {
        this.detectors = Collections.unmodifiableList(detectors);
        this.bucketSpan = bucketSpan;
        this.latency = latency;
        this.categorizationFieldName = categorizationFieldName;
        this.categorizationAnalyzerConfig = categorizationAnalyzerConfig;
        this.categorizationFilters = categorizationFilters == null ? null : Collections.unmodifiableList(categorizationFilters);
        this.summaryCountFieldName = summaryCountFieldName;
        this.influencers = Collections.unmodifiableList(influencers);
        this.multivariateByFields = multivariateByFields;
    }

    /**
     * The analysis bucket span
     *
     * @return The bucketspan or <code>null</code> if not set
     */
    public TimeValue getBucketSpan() {
        return bucketSpan;
    }

    public String getCategorizationFieldName() {
        return categorizationFieldName;
    }

    public List<String> getCategorizationFilters() {
        return categorizationFilters;
    }

    public CategorizationAnalyzerConfig getCategorizationAnalyzerConfig() {
        return categorizationAnalyzerConfig;
    }

    /**
     * The latency interval during which out-of-order records should be handled.
     *
     * @return The latency interval or <code>null</code> if not set
     */
    public TimeValue getLatency() {
        return latency;
    }

    /**
     * The name of the field that contains counts for pre-summarised input
     *
     * @return The field name or <code>null</code> if not set
     */
    public String getSummaryCountFieldName() {
        return summaryCountFieldName;
    }

    /**
     * The list of analysis detectors. In a valid configuration the list should
     * contain at least 1 {@link Detector}
     *
     * @return The Detectors used in this job
     */
    public List<Detector> getDetectors() {
        return detectors;
    }

    /**
     * The list of influence field names
     */
    public List<String> getInfluencers() {
        return influencers;
    }

    public Boolean getMultivariateByFields() {
        return multivariateByFields;
    }

    private static void addIfNotNull(Set<String> fields, String field) {
        if (field != null) {
            fields.add(field);
        }
    }

    public List<String> fields() {
        return collectNonNullAndNonEmptyDetectorFields(Detector::getFieldName);
    }

    private List<String> collectNonNullAndNonEmptyDetectorFields(
        Function<Detector, String> fieldGetter) {
        Set<String> fields = new HashSet<>();

        for (Detector d : getDetectors()) {
            addIfNotNull(fields, fieldGetter.apply(d));
        }

        // remove empty strings
        fields.remove("");

        return new ArrayList<>(fields);
    }

    public List<String> byFields() {
        return collectNonNullAndNonEmptyDetectorFields(Detector::getByFieldName);
    }

    public List<String> overFields() {
        return collectNonNullAndNonEmptyDetectorFields(Detector::getOverFieldName);
    }

    public List<String> partitionFields() {
        return collectNonNullAndNonEmptyDetectorFields(Detector::getPartitionFieldName);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (bucketSpan != null) {
            builder.field(BUCKET_SPAN.getPreferredName(), bucketSpan.getStringRep());
        }
        if (categorizationFieldName != null) {
            builder.field(CATEGORIZATION_FIELD_NAME.getPreferredName(), categorizationFieldName);
        }
        if (categorizationFilters != null) {
            builder.field(CATEGORIZATION_FILTERS.getPreferredName(), categorizationFilters);
        }
        if (categorizationAnalyzerConfig != null) {
            // This cannot be builder.field(CATEGORIZATION_ANALYZER.getPreferredName(), categorizationAnalyzerConfig, params);
            // because that always writes categorizationAnalyzerConfig as an object, and in the case of a global analyzer it
            // gets written as a single string.
            categorizationAnalyzerConfig.toXContent(builder, params);
        }
        if (latency != null) {
            builder.field(LATENCY.getPreferredName(), latency.getStringRep());
        }
        if (summaryCountFieldName != null) {
            builder.field(SUMMARY_COUNT_FIELD_NAME.getPreferredName(), summaryCountFieldName);
        }
        builder.startArray(DETECTORS.getPreferredName());
        for (Detector detector : detectors) {
            detector.toXContent(builder, params);
        }
        builder.endArray();
        builder.field(INFLUENCERS.getPreferredName(), influencers);
        if (multivariateByFields != null) {
            builder.field(MULTIVARIATE_BY_FIELDS.getPreferredName(), multivariateByFields);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        AnalysisConfig that = (AnalysisConfig) object;
        return Objects.equals(latency, that.latency) &&
            Objects.equals(bucketSpan, that.bucketSpan) &&
            Objects.equals(categorizationFieldName, that.categorizationFieldName) &&
            Objects.equals(categorizationFilters, that.categorizationFilters) &&
            Objects.equals(categorizationAnalyzerConfig, that.categorizationAnalyzerConfig) &&
            Objects.equals(summaryCountFieldName, that.summaryCountFieldName) &&
            Objects.equals(detectors, that.detectors) &&
            Objects.equals(influencers, that.influencers) &&
            Objects.equals(multivariateByFields, that.multivariateByFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            bucketSpan, categorizationFieldName, categorizationFilters, categorizationAnalyzerConfig, latency,
            summaryCountFieldName, detectors, influencers, multivariateByFields);
    }

    public static Builder builder(List<Detector> detectors) {
        return new Builder(detectors);
    }

    public static class Builder {

        private List<Detector> detectors;
        private TimeValue bucketSpan;
        private TimeValue latency;
        private String categorizationFieldName;
        private List<String> categorizationFilters;
        private CategorizationAnalyzerConfig categorizationAnalyzerConfig;
        private String summaryCountFieldName;
        private List<String> influencers = new ArrayList<>();
        private Boolean multivariateByFields;

        public Builder(List<Detector> detectors) {
            setDetectors(detectors);
        }

        public Builder(AnalysisConfig analysisConfig) {
            this.detectors = new ArrayList<>(analysisConfig.detectors);
            this.bucketSpan = analysisConfig.bucketSpan;
            this.latency = analysisConfig.latency;
            this.categorizationFieldName = analysisConfig.categorizationFieldName;
            this.categorizationFilters = analysisConfig.categorizationFilters == null ? null
                : new ArrayList<>(analysisConfig.categorizationFilters);
            this.categorizationAnalyzerConfig = analysisConfig.categorizationAnalyzerConfig;
            this.summaryCountFieldName = analysisConfig.summaryCountFieldName;
            this.influencers = new ArrayList<>(analysisConfig.influencers);
            this.multivariateByFields = analysisConfig.multivariateByFields;
        }

        public Builder setDetectors(List<Detector> detectors) {
            Objects.requireNonNull(detectors,  "[" + DETECTORS.getPreferredName() + "] must not be null");
            // We always assign sequential IDs to the detectors that are correct for this analysis config
            int detectorIndex = 0;
            List<Detector> sequentialIndexDetectors = new ArrayList<>(detectors.size());
            for (Detector origDetector : detectors) {
                Detector.Builder builder = new Detector.Builder(origDetector);
                builder.setDetectorIndex(detectorIndex++);
                sequentialIndexDetectors.add(builder.build());
            }
            this.detectors = sequentialIndexDetectors;
            return this;
        }

        public Builder setDetector(int detectorIndex, Detector detector) {
            detectors.set(detectorIndex, detector);
            return this;
        }

        public Builder setBucketSpan(TimeValue bucketSpan) {
            this.bucketSpan = bucketSpan;
            return this;
        }

        public Builder setLatency(TimeValue latency) {
            this.latency = latency;
            return this;
        }

        public Builder setCategorizationFieldName(String categorizationFieldName) {
            this.categorizationFieldName = categorizationFieldName;
            return this;
        }

        public Builder setCategorizationFilters(List<String> categorizationFilters) {
            this.categorizationFilters = categorizationFilters;
            return this;
        }

        public Builder setCategorizationAnalyzerConfig(CategorizationAnalyzerConfig categorizationAnalyzerConfig) {
            this.categorizationAnalyzerConfig = categorizationAnalyzerConfig;
            return this;
        }

        public Builder setSummaryCountFieldName(String summaryCountFieldName) {
            this.summaryCountFieldName = summaryCountFieldName;
            return this;
        }

        public Builder setInfluencers(List<String> influencers) {
            this.influencers = Objects.requireNonNull(influencers, INFLUENCERS.getPreferredName());
            return this;
        }

        public Builder setMultivariateByFields(Boolean multivariateByFields) {
            this.multivariateByFields = multivariateByFields;
            return this;
        }

        public AnalysisConfig build() {

            return new AnalysisConfig(bucketSpan, categorizationFieldName, categorizationFilters, categorizationAnalyzerConfig,
                latency, summaryCountFieldName, detectors, influencers, multivariateByFields);
        }
    }
}
