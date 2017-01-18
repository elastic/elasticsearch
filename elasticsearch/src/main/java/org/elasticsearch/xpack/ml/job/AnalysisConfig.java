/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.job.messages.Messages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;


/**
 * Autodetect analysis configuration options describes which fields are
 * analysed and the functions to use.
 * <p>
 * The configuration can contain multiple detectors, a new anomaly detector will
 * be created for each detector configuration. The fields
 * <code>bucketSpan, batchSpan, summaryCountFieldName and categorizationFieldName</code>
 * apply to all detectors.
 * <p>
 * If a value has not been set it will be <code>null</code>
 * Object wrappers are used around integral types &amp; booleans so they can take
 * <code>null</code> values.
 */
public class AnalysisConfig extends ToXContentToBytes implements Writeable {
    /**
     * Serialisation names
     */
    private static final ParseField ANALYSIS_CONFIG = new ParseField("analysis_config");
    private static final ParseField BUCKET_SPAN = new ParseField("bucket_span");
    private static final ParseField BATCH_SPAN = new ParseField("batch_span");
    private static final ParseField CATEGORIZATION_FIELD_NAME = new ParseField("categorization_field_name");
    private static final ParseField CATEGORIZATION_FILTERS = new ParseField("categorization_filters");
    private static final ParseField LATENCY = new ParseField("latency");
    private static final ParseField PERIOD = new ParseField("period");
    private static final ParseField SUMMARY_COUNT_FIELD_NAME = new ParseField("summary_count_field_name");
    private static final ParseField DETECTORS = new ParseField("detectors");
    private static final ParseField INFLUENCERS = new ParseField("influencers");
    private static final ParseField OVERLAPPING_BUCKETS = new ParseField("overlapping_buckets");
    private static final ParseField RESULT_FINALIZATION_WINDOW = new ParseField("result_finalization_window");
    private static final ParseField MULTIVARIATE_BY_FIELDS = new ParseField("multivariate_by_fields");
    private static final ParseField MULTIPLE_BUCKET_SPANS = new ParseField("multiple_bucket_spans");
    private static final ParseField USER_PER_PARTITION_NORMALIZATION = new ParseField("use_per_partition_normalization");

    private static final String ML_CATEGORY_FIELD = "mlcategory";
    public static final Set<String> AUTO_CREATED_FIELDS = new HashSet<>(Arrays.asList(ML_CATEGORY_FIELD));

    public static final long DEFAULT_RESULT_FINALIZATION_WINDOW = 2L;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<AnalysisConfig.Builder, Void> PARSER =
            new ConstructingObjectParser<>(ANALYSIS_CONFIG.getPreferredName(), a -> new AnalysisConfig.Builder((List<Detector>) a[0]));

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> Detector.PARSER.apply(p, c).build(), DETECTORS);
        PARSER.declareLong(Builder::setBucketSpan, BUCKET_SPAN);
        PARSER.declareLong(Builder::setBatchSpan, BATCH_SPAN);
        PARSER.declareString(Builder::setCategorizationFieldName, CATEGORIZATION_FIELD_NAME);
        PARSER.declareStringArray(Builder::setCategorizationFilters, CATEGORIZATION_FILTERS);
        PARSER.declareLong(Builder::setLatency, LATENCY);
        PARSER.declareLong(Builder::setPeriod, PERIOD);
        PARSER.declareString(Builder::setSummaryCountFieldName, SUMMARY_COUNT_FIELD_NAME);
        PARSER.declareStringArray(Builder::setInfluencers, INFLUENCERS);
        PARSER.declareBoolean(Builder::setOverlappingBuckets, OVERLAPPING_BUCKETS);
        PARSER.declareLong(Builder::setResultFinalizationWindow, RESULT_FINALIZATION_WINDOW);
        PARSER.declareBoolean(Builder::setMultivariateByFields, MULTIVARIATE_BY_FIELDS);
        PARSER.declareLongArray(Builder::setMultipleBucketSpans, MULTIPLE_BUCKET_SPANS);
        PARSER.declareBoolean(Builder::setUsePerPartitionNormalization, USER_PER_PARTITION_NORMALIZATION);
    }

    /**
     * These values apply to all detectors
     */
    private final long bucketSpan;
    private final Long batchSpan;
    private final String categorizationFieldName;
    private final List<String> categorizationFilters;
    private final long latency;
    private final Long period;
    private final String summaryCountFieldName;
    private final List<Detector> detectors;
    private final List<String> influencers;
    private final Boolean overlappingBuckets;
    private final Long resultFinalizationWindow;
    private final Boolean multivariateByFields;
    private final List<Long> multipleBucketSpans;
    private final boolean usePerPartitionNormalization;

    private AnalysisConfig(Long bucketSpan, Long batchSpan, String categorizationFieldName, List<String> categorizationFilters,
                           long latency, Long period, String summaryCountFieldName, List<Detector> detectors, List<String> influencers,
                           Boolean overlappingBuckets, Long resultFinalizationWindow, Boolean multivariateByFields,
                           List<Long> multipleBucketSpans, boolean usePerPartitionNormalization) {
        this.detectors = detectors;
        this.bucketSpan = bucketSpan;
        this.batchSpan = batchSpan;
        this.latency = latency;
        this.period = period;
        this.categorizationFieldName = categorizationFieldName;
        this.categorizationFilters = categorizationFilters;
        this.summaryCountFieldName = summaryCountFieldName;
        this.influencers = influencers;
        this.overlappingBuckets = overlappingBuckets;
        this.resultFinalizationWindow = resultFinalizationWindow;
        this.multivariateByFields = multivariateByFields;
        this.multipleBucketSpans = multipleBucketSpans;
        this.usePerPartitionNormalization = usePerPartitionNormalization;
    }

    public AnalysisConfig(StreamInput in) throws IOException {
        bucketSpan = in.readLong();
        batchSpan = in.readOptionalLong();
        categorizationFieldName = in.readOptionalString();
        categorizationFilters = in.readBoolean() ? in.readList(StreamInput::readString) : null;
        latency = in.readLong();
        period = in.readOptionalLong();
        summaryCountFieldName = in.readOptionalString();
        detectors = in.readList(Detector::new);
        influencers = in.readList(StreamInput::readString);
        overlappingBuckets = in.readOptionalBoolean();
        resultFinalizationWindow = in.readOptionalLong();
        multivariateByFields = in.readOptionalBoolean();
        multipleBucketSpans = in.readBoolean() ? in.readList(StreamInput::readLong) : null;
        usePerPartitionNormalization = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(bucketSpan);
        out.writeOptionalLong(batchSpan);
        out.writeOptionalString(categorizationFieldName);
        if (categorizationFilters != null) {
            out.writeBoolean(true);
            out.writeStringList(categorizationFilters);
        } else {
            out.writeBoolean(false);
        }
        out.writeLong(latency);
        out.writeOptionalLong(period);
        out.writeOptionalString(summaryCountFieldName);
        out.writeList(detectors);
        out.writeStringList(influencers);
        out.writeOptionalBoolean(overlappingBuckets);
        out.writeOptionalLong(resultFinalizationWindow);
        out.writeOptionalBoolean(multivariateByFields);
        if (multipleBucketSpans != null) {
            out.writeBoolean(true);
            out.writeVInt(multipleBucketSpans.size());
            for (Long bucketSpan : multipleBucketSpans) {
                out.writeLong(bucketSpan);
            }
        } else {
            out.writeBoolean(false);
        }
        out.writeBoolean(usePerPartitionNormalization);
    }

    /**
     * The size of the interval the analysis is aggregated into measured in
     * seconds
     *
     * @return The bucketspan or <code>null</code> if not set
     */
    public Long getBucketSpan() {
        return bucketSpan;
    }

    public long getBucketSpanOrDefault() {
        return bucketSpan;
    }

    /**
     * Interval into which to batch seasonal data measured in seconds
     *
     * @return The batchspan or <code>null</code> if not set
     */
    public Long getBatchSpan() {
        return batchSpan;
    }

    public String getCategorizationFieldName() {
        return categorizationFieldName;
    }

    public List<String> getCategorizationFilters() {
        return categorizationFilters;
    }

    /**
     * The latency interval (seconds) during which out-of-order records should be handled.
     *
     * @return The latency interval (seconds) or <code>null</code> if not set
     */
    public Long getLatency() {
        return latency;
    }

    /**
     * The repeat interval for periodic data in multiples of
     * {@linkplain #getBatchSpan()}
     *
     * @return The period or <code>null</code> if not set
     */
    public Long getPeriod() {
        return period;
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

    /**
     * Return the list of term fields.
     * These are the influencer fields, partition field,
     * by field and over field of each detector.
     * <code>null</code> and empty strings are filtered from the
     * config.
     *
     * @return Set of term fields - never <code>null</code>
     */
    public Set<String> termFields() {
        Set<String> termFields = new TreeSet<>();

        for (Detector d : getDetectors()) {
            addIfNotNull(termFields, d.getByFieldName());
            addIfNotNull(termFields, d.getOverFieldName());
            addIfNotNull(termFields, d.getPartitionFieldName());
        }

        for (String i : getInfluencers()) {
            addIfNotNull(termFields, i);
        }

        // remove empty strings
        termFields.remove("");

        return termFields;
    }

    public Set<String> extractReferencedLists() {
        return detectors.stream().map(Detector::extractReferencedLists)
                .flatMap(Set::stream).collect(Collectors.toSet());
    }

    public Boolean getOverlappingBuckets() {
        return overlappingBuckets;
    }

    public Long getResultFinalizationWindow() {
        return resultFinalizationWindow;
    }

    public Boolean getMultivariateByFields() {
        return multivariateByFields;
    }

    public List<Long> getMultipleBucketSpans() {
        return multipleBucketSpans;
    }

    public boolean getUsePerPartitionNormalization() {
        return usePerPartitionNormalization;
    }

    /**
     * Return the list of fields required by the analysis.
     * These are the influencer fields, metric field, partition field,
     * by field and over field of each detector, plus the summary count
     * field and the categorization field name of the job.
     * <code>null</code> and empty strings are filtered from the
     * config.
     *
     * @return List of required analysis fields - never <code>null</code>
     */
    public List<String> analysisFields() {
        Set<String> analysisFields = termFields();

        addIfNotNull(analysisFields, categorizationFieldName);
        addIfNotNull(analysisFields, summaryCountFieldName);

        for (Detector d : getDetectors()) {
            addIfNotNull(analysisFields, d.getFieldName());
        }

        // remove empty strings
        analysisFields.remove("");

        return new ArrayList<>(analysisFields);
    }

    private static void addIfNotNull(Set<String> fields, String field) {
        if (field != null) {
            fields.add(field);
        }
    }

    public List<String> fields() {
        return collectNonNullAndNonEmptyDetectorFields(d -> d.getFieldName());
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
        return collectNonNullAndNonEmptyDetectorFields(d -> d.getByFieldName());
    }

    public List<String> overFields() {
        return collectNonNullAndNonEmptyDetectorFields(d -> d.getOverFieldName());
    }


    public List<String> partitionFields() {
        return collectNonNullAndNonEmptyDetectorFields(d -> d.getPartitionFieldName());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(BUCKET_SPAN.getPreferredName(), bucketSpan);
        if (batchSpan != null) {
            builder.field(BATCH_SPAN.getPreferredName(), batchSpan);
        }
        if (categorizationFieldName != null) {
            builder.field(CATEGORIZATION_FIELD_NAME.getPreferredName(), categorizationFieldName);
        }
        if (categorizationFilters != null) {
            builder.field(CATEGORIZATION_FILTERS.getPreferredName(), categorizationFilters);
        }
        builder.field(LATENCY.getPreferredName(), latency);
        if (period != null) {
            builder.field(PERIOD.getPreferredName(), period);
        }
        if (summaryCountFieldName != null) {
            builder.field(SUMMARY_COUNT_FIELD_NAME.getPreferredName(), summaryCountFieldName);
        }
        builder.field(DETECTORS.getPreferredName(), detectors);
        builder.field(INFLUENCERS.getPreferredName(), influencers);
        if (overlappingBuckets != null) {
            builder.field(OVERLAPPING_BUCKETS.getPreferredName(), overlappingBuckets);
        }
        if (resultFinalizationWindow != null) {
            builder.field(RESULT_FINALIZATION_WINDOW.getPreferredName(), resultFinalizationWindow);
        }
        if (multivariateByFields != null) {
            builder.field(MULTIVARIATE_BY_FIELDS.getPreferredName(), multivariateByFields);
        }
        if (multipleBucketSpans != null) {
            builder.field(MULTIPLE_BUCKET_SPANS.getPreferredName(), multipleBucketSpans);
        }
        builder.field(USER_PER_PARTITION_NORMALIZATION.getPreferredName(), usePerPartitionNormalization);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalysisConfig that = (AnalysisConfig) o;
        return latency == that.latency &&
                usePerPartitionNormalization == that.usePerPartitionNormalization &&
                Objects.equals(bucketSpan, that.bucketSpan) &&
                Objects.equals(batchSpan, that.batchSpan) &&
                Objects.equals(categorizationFieldName, that.categorizationFieldName) &&
                Objects.equals(categorizationFilters, that.categorizationFilters) &&
                Objects.equals(period, that.period) &&
                Objects.equals(summaryCountFieldName, that.summaryCountFieldName) &&
                Objects.equals(detectors, that.detectors) &&
                Objects.equals(influencers, that.influencers) &&
                Objects.equals(overlappingBuckets, that.overlappingBuckets) &&
                Objects.equals(resultFinalizationWindow, that.resultFinalizationWindow) &&
                Objects.equals(multivariateByFields, that.multivariateByFields) &&
                Objects.equals(multipleBucketSpans, that.multipleBucketSpans);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                bucketSpan, batchSpan, categorizationFieldName, categorizationFilters, latency, period,
                summaryCountFieldName, detectors, influencers, overlappingBuckets, resultFinalizationWindow,
                multivariateByFields, multipleBucketSpans, usePerPartitionNormalization
        );
    }

    public static class Builder {

        public static final long DEFAULT_BUCKET_SPAN = 300L;

        private List<Detector> detectors;
        private long bucketSpan = DEFAULT_BUCKET_SPAN;
        private Long batchSpan;
        private long latency = 0L;
        private Long period;
        private String categorizationFieldName;
        private List<String> categorizationFilters;
        private String summaryCountFieldName;
        private List<String> influencers = new ArrayList<>();
        private Boolean overlappingBuckets;
        private Long resultFinalizationWindow;
        private Boolean multivariateByFields;
        private List<Long> multipleBucketSpans;
        private boolean usePerPartitionNormalization = false;

        public Builder(List<Detector> detectors) {
            this.detectors = detectors;
        }

        public Builder(AnalysisConfig analysisConfig) {
            this.detectors = analysisConfig.detectors;
            this.bucketSpan = analysisConfig.bucketSpan;
            this.batchSpan = analysisConfig.batchSpan;
            this.latency = analysisConfig.latency;
            this.period = analysisConfig.period;
            this.categorizationFieldName = analysisConfig.categorizationFieldName;
            this.categorizationFilters = analysisConfig.categorizationFilters;
            this.summaryCountFieldName = analysisConfig.summaryCountFieldName;
            this.influencers = analysisConfig.influencers;
            this.overlappingBuckets = analysisConfig.overlappingBuckets;
            this.resultFinalizationWindow = analysisConfig.resultFinalizationWindow;
            this.multivariateByFields = analysisConfig.multivariateByFields;
            this.multipleBucketSpans = analysisConfig.multipleBucketSpans;
            this.usePerPartitionNormalization = analysisConfig.usePerPartitionNormalization;
        }

        public void setDetectors(List<Detector> detectors) {
            this.detectors = detectors;
        }

        public void setBucketSpan(long bucketSpan) {
            this.bucketSpan = bucketSpan;
        }

        public void setBatchSpan(long batchSpan) {
            this.batchSpan = batchSpan;
        }

        public void setLatency(long latency) {
            this.latency = latency;
        }

        public void setPeriod(long period) {
            this.period = period;
        }

        public void setCategorizationFieldName(String categorizationFieldName) {
            this.categorizationFieldName = categorizationFieldName;
        }

        public void setCategorizationFilters(List<String> categorizationFilters) {
            this.categorizationFilters = categorizationFilters;
        }

        public void setSummaryCountFieldName(String summaryCountFieldName) {
            this.summaryCountFieldName = summaryCountFieldName;
        }

        public void setInfluencers(List<String> influencers) {
            this.influencers = influencers;
        }

        public void setOverlappingBuckets(Boolean overlappingBuckets) {
            this.overlappingBuckets = overlappingBuckets;
        }

        public void setResultFinalizationWindow(Long resultFinalizationWindow) {
            this.resultFinalizationWindow = resultFinalizationWindow;
        }

        public void setMultivariateByFields(Boolean multivariateByFields) {
            this.multivariateByFields = multivariateByFields;
        }

        public void setMultipleBucketSpans(List<Long> multipleBucketSpans) {
            this.multipleBucketSpans = multipleBucketSpans;
        }

        public void setUsePerPartitionNormalization(boolean usePerPartitionNormalization) {
            this.usePerPartitionNormalization = usePerPartitionNormalization;
        }

        /**
         * Checks the configuration is valid
         * <ol>
         * <li>Check that if non-null BucketSpan, BatchSpan, Latency and Period are
         * &gt;= 0</li>
         * <li>Check that if non-null Latency is &lt;= MAX_LATENCY</li>
         * <li>Check there is at least one detector configured</li>
         * <li>Check all the detectors are configured correctly</li>
         * <li>Check that OVERLAPPING_BUCKETS is set appropriately</li>
         * <li>Check that MULTIPLE_BUCKETSPANS are set appropriately</li>
         * <li>If Per Partition normalization is configured at least one detector
         * must have a partition field and no influences can be used</li>
         * </ol>
         */
        public AnalysisConfig build() {
            checkFieldIsNotNegativeIfSpecified(BUCKET_SPAN.getPreferredName(), bucketSpan);
            checkFieldIsNotNegativeIfSpecified(BATCH_SPAN.getPreferredName(), batchSpan);
            checkFieldIsNotNegativeIfSpecified(LATENCY.getPreferredName(), latency);
            checkFieldIsNotNegativeIfSpecified(PERIOD.getPreferredName(), period);

            verifyDetectorAreDefined(detectors);
            verifyFieldName(summaryCountFieldName);
            verifyFieldName(categorizationFieldName);

            verifyCategorizationFilters(categorizationFilters, categorizationFieldName);
            verifyMultipleBucketSpans(multipleBucketSpans, bucketSpan);

            overlappingBuckets = verifyOverlappingBucketsConfig(overlappingBuckets, detectors);

            if (usePerPartitionNormalization) {
                checkDetectorsHavePartitionFields(detectors);
                checkNoInfluencersAreSet(influencers);
            }

            return new AnalysisConfig(bucketSpan, batchSpan, categorizationFieldName, categorizationFilters,
                    latency, period, summaryCountFieldName, detectors, influencers, overlappingBuckets,
                    resultFinalizationWindow, multivariateByFields, multipleBucketSpans, usePerPartitionNormalization);
        }

        private static void checkFieldIsNotNegativeIfSpecified(String fieldName, Long value) {
            if (value != null && value < 0) {
                String msg = Messages.getMessage(Messages.JOB_CONFIG_FIELD_VALUE_TOO_LOW, fieldName, 0, value);
                throw new IllegalArgumentException(msg);
            }
        }

        private static void verifyDetectorAreDefined(List<Detector> detectors) {
            if (detectors == null || detectors.isEmpty()) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_NO_DETECTORS));
            }
        }

        private static void verifyCategorizationFilters(List<String> filters, String categorizationFieldName) {
            if (filters == null || filters.isEmpty()) {
                return;
            }

            verifyCategorizationFieldNameSetIfFiltersAreSet(categorizationFieldName);
            verifyCategorizationFiltersAreDistinct(filters);
            verifyCategorizationFiltersContainNoneEmpty(filters);
            verifyCategorizationFiltersAreValidRegex(filters);
        }

        private static void verifyCategorizationFieldNameSetIfFiltersAreSet(String categorizationFieldName) {
            if (categorizationFieldName == null) {
                throw new IllegalArgumentException(Messages.getMessage(
                        Messages.JOB_CONFIG_CATEGORIZATION_FILTERS_REQUIRE_CATEGORIZATION_FIELD_NAME));
            }
        }

        private static void verifyCategorizationFiltersAreDistinct(List<String> filters) {
            if (filters.stream().distinct().count() != filters.size()) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_CATEGORIZATION_FILTERS_CONTAINS_DUPLICATES));
            }
        }

        private static void verifyCategorizationFiltersContainNoneEmpty(List<String> filters) {
            if (filters.stream().anyMatch(f -> f.isEmpty())) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_CATEGORIZATION_FILTERS_CONTAINS_EMPTY));
            }
        }

        private static void verifyCategorizationFiltersAreValidRegex(List<String> filters) {
            for (String filter : filters) {
                if (!isValidRegex(filter)) {
                    throw new IllegalArgumentException(
                            Messages.getMessage(Messages.JOB_CONFIG_CATEGORIZATION_FILTERS_CONTAINS_INVALID_REGEX, filter));
                }
            }
        }

        private static void verifyMultipleBucketSpans(List<Long> multipleBucketSpans, Long bucketSpan) {
            if (multipleBucketSpans == null) {
                return;
            }

            if (bucketSpan == null) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_MULTIPLE_BUCKETSPANS_REQUIRE_BUCKETSPAN));
            }
            for (Long span : multipleBucketSpans) {
                if ((span % bucketSpan != 0L) || (span <= bucketSpan)) {
                    throw new IllegalArgumentException(
                            Messages.getMessage(Messages.JOB_CONFIG_MULTIPLE_BUCKETSPANS_MUST_BE_MULTIPLE, span, bucketSpan));
                }
            }
        }

        private static boolean checkDetectorsHavePartitionFields(List<Detector> detectors) {
            for (Detector detector : detectors) {
                if (!Strings.isNullOrEmpty(detector.getPartitionFieldName())) {
                    return true;
                }
            }
            throw new IllegalArgumentException(Messages.getMessage(
                    Messages.JOB_CONFIG_PER_PARTITION_NORMALIZATION_REQUIRES_PARTITION_FIELD));
        }

        private static boolean checkNoInfluencersAreSet(List<String> influencers) {
            if (!influencers.isEmpty()) {
                throw new IllegalArgumentException(Messages.getMessage(
                        Messages.JOB_CONFIG_PER_PARTITION_NORMALIZATION_CANNOT_USE_INFLUENCERS));
            }

            return true;
        }

        /**
         * Check that the characters used in a field name will not cause problems.
         *
         * @param field The field name to be validated
         * @return true
         */
        public static boolean verifyFieldName(String field) throws ElasticsearchParseException {
            if (field != null && containsInvalidChar(field)) {
                throw new IllegalArgumentException(
                        Messages.getMessage(Messages.JOB_CONFIG_INVALID_FIELDNAME_CHARS, field, Detector.PROHIBITED));
            }
            return true;
        }

        private static boolean containsInvalidChar(String field) {
            for (Character ch : Detector.PROHIBITED_FIELDNAME_CHARACTERS) {
                if (field.indexOf(ch) >= 0) {
                    return true;
                }
            }
            return field.chars().anyMatch(ch -> Character.isISOControl(ch));
        }

        private static boolean isValidRegex(String exp) {
            try {
                Pattern.compile(exp);
                return true;
            } catch (PatternSyntaxException e) {
                return false;
            }
        }

        private static Boolean verifyOverlappingBucketsConfig(Boolean overlappingBuckets, List<Detector> detectors) {
            // If any detector function is rare/freq_rare, mustn't use overlapping buckets
            boolean mustNotUse = false;

            List<String> illegalFunctions = new ArrayList<>();
            for (Detector d : detectors) {
                if (Detector.NO_OVERLAPPING_BUCKETS_FUNCTIONS.contains(d.getFunction())) {
                    illegalFunctions.add(d.getFunction());
                    mustNotUse = true;
                }
            }

            if (Boolean.TRUE.equals(overlappingBuckets) && mustNotUse) {
                throw new IllegalArgumentException(
                        Messages.getMessage(Messages.JOB_CONFIG_OVERLAPPING_BUCKETS_INCOMPATIBLE_FUNCTION, illegalFunctions.toString()));
            }

            return overlappingBuckets;
        }
    }
}
