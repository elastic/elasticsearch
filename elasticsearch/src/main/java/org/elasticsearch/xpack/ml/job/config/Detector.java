/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.job.messages.Messages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Defines the fields to be used in the analysis.
 * <code>fieldname</code> must be set and only one of <code>byFieldName</code>
 * and <code>overFieldName</code> should be set.
 */
public class Detector extends ToXContentToBytes implements Writeable {

    public enum ExcludeFrequent implements Writeable {
        ALL("all"),
        NONE("none"),
        BY("by"),
        OVER("over");

        private final String token;

        ExcludeFrequent(String token) {
            this.token = token;
        }

        public String getToken() {
            return token;
        }

        /**
         * Case-insensitive from string method.
         * Works with either JSON, json, etc.
         *
         * @param value String representation
         * @return The data format
         */
        public static ExcludeFrequent forString(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

        public static ExcludeFrequent readFromStream(StreamInput in) throws IOException {
            int ordinal = in.readVInt();
            if (ordinal < 0 || ordinal >= values().length) {
                throw new IOException("Unknown ExcludeFrequent ordinal [" + ordinal + "]");
            }
            return values()[ordinal];
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(ordinal());
        }
    }

    public static final ParseField DETECTOR_DESCRIPTION_FIELD = new ParseField("detector_description");
    public static final ParseField FUNCTION_FIELD = new ParseField("function");
    public static final ParseField FIELD_NAME_FIELD = new ParseField("field_name");
    public static final ParseField BY_FIELD_NAME_FIELD = new ParseField("by_field_name");
    public static final ParseField OVER_FIELD_NAME_FIELD = new ParseField("over_field_name");
    public static final ParseField PARTITION_FIELD_NAME_FIELD = new ParseField("partition_field_name");
    public static final ParseField USE_NULL_FIELD = new ParseField("use_null");
    public static final ParseField EXCLUDE_FREQUENT_FIELD = new ParseField("exclude_frequent");
    public static final ParseField DETECTOR_RULES_FIELD = new ParseField("detector_rules");

    public static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("detector", Builder::new);

    static {
        PARSER.declareString(Builder::setDetectorDescription, DETECTOR_DESCRIPTION_FIELD);
        PARSER.declareString(Builder::setFunction, FUNCTION_FIELD);
        PARSER.declareString(Builder::setFieldName, FIELD_NAME_FIELD);
        PARSER.declareString(Builder::setByFieldName, BY_FIELD_NAME_FIELD);
        PARSER.declareString(Builder::setOverFieldName, OVER_FIELD_NAME_FIELD);
        PARSER.declareString(Builder::setPartitionFieldName, PARTITION_FIELD_NAME_FIELD);
        PARSER.declareBoolean(Builder::setUseNull, USE_NULL_FIELD);
        PARSER.declareField(Builder::setExcludeFrequent, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return ExcludeFrequent.forString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, EXCLUDE_FREQUENT_FIELD, ObjectParser.ValueType.STRING);
        PARSER.declareObjectArray(Builder::setDetectorRules, DetectionRule.PARSER, DETECTOR_RULES_FIELD);
    }

    public static final String COUNT = "count";
    public static final String HIGH_COUNT = "high_count";
    public static final String LOW_COUNT = "low_count";
    public static final String NON_ZERO_COUNT = "non_zero_count";
    public static final String LOW_NON_ZERO_COUNT = "low_non_zero_count";
    public static final String HIGH_NON_ZERO_COUNT = "high_non_zero_count";
    public static final String NZC = "nzc";
    public static final String LOW_NZC = "low_nzc";
    public static final String HIGH_NZC = "high_nzc";
    public static final String DISTINCT_COUNT = "distinct_count";
    public static final String LOW_DISTINCT_COUNT = "low_distinct_count";
    public static final String HIGH_DISTINCT_COUNT = "high_distinct_count";
    public static final String DC = "dc";
    public static final String LOW_DC = "low_dc";
    public static final String HIGH_DC = "high_dc";
    public static final String RARE = "rare";
    public static final String FREQ_RARE = "freq_rare";
    public static final String INFO_CONTENT = "info_content";
    public static final String LOW_INFO_CONTENT = "low_info_content";
    public static final String HIGH_INFO_CONTENT = "high_info_content";
    public static final String METRIC = "metric";
    public static final String MEAN = "mean";
    public static final String MEDIAN = "median";
    public static final String HIGH_MEAN = "high_mean";
    public static final String LOW_MEAN = "low_mean";
    public static final String AVG = "avg";
    public static final String HIGH_AVG = "high_avg";
    public static final String LOW_AVG = "low_avg";
    public static final String MIN = "min";
    public static final String MAX = "max";
    public static final String SUM = "sum";
    public static final String LOW_SUM = "low_sum";
    public static final String HIGH_SUM = "high_sum";
    public static final String NON_NULL_SUM = "non_null_sum";
    public static final String LOW_NON_NULL_SUM = "low_non_null_sum";
    public static final String HIGH_NON_NULL_SUM = "high_non_null_sum";
    /**
     * Population variance is called varp to match Splunk
     */
    public static final String POPULATION_VARIANCE = "varp";
    public static final String LOW_POPULATION_VARIANCE = "low_varp";
    public static final String HIGH_POPULATION_VARIANCE = "high_varp";
    public static final String TIME_OF_DAY = "time_of_day";
    public static final String TIME_OF_WEEK = "time_of_week";
    public static final String LAT_LONG = "lat_long";


    /**
     * The set of valid function names.
     */
    public static final Set<String> ANALYSIS_FUNCTIONS =
            new HashSet<>(Arrays.asList(
                    // The convention here is that synonyms (only) go on the same line
                    COUNT,
                    HIGH_COUNT,
                    LOW_COUNT,
                    NON_ZERO_COUNT, NZC,
                    LOW_NON_ZERO_COUNT, LOW_NZC,
                    HIGH_NON_ZERO_COUNT, HIGH_NZC,
                    DISTINCT_COUNT, DC,
                    LOW_DISTINCT_COUNT, LOW_DC,
                    HIGH_DISTINCT_COUNT, HIGH_DC,
                    RARE,
                    FREQ_RARE,
                    INFO_CONTENT,
                    LOW_INFO_CONTENT,
                    HIGH_INFO_CONTENT,
                    METRIC,
                    MEAN, AVG,
                    HIGH_MEAN, HIGH_AVG,
                    LOW_MEAN, LOW_AVG,
                    MEDIAN,
                    MIN,
                    MAX,
                    SUM,
                    LOW_SUM,
                    HIGH_SUM,
                    NON_NULL_SUM,
                    LOW_NON_NULL_SUM,
                    HIGH_NON_NULL_SUM,
                    POPULATION_VARIANCE,
                    LOW_POPULATION_VARIANCE,
                    HIGH_POPULATION_VARIANCE,
                    TIME_OF_DAY,
                    TIME_OF_WEEK,
                    LAT_LONG
                    ));

    /**
     * The set of functions that do not require a field, by field or over field
     */
    public static final Set<String> COUNT_WITHOUT_FIELD_FUNCTIONS =
            new HashSet<>(Arrays.asList(
                    COUNT,
                    HIGH_COUNT,
                    LOW_COUNT,
                    NON_ZERO_COUNT, NZC,
                    LOW_NON_ZERO_COUNT, LOW_NZC,
                    HIGH_NON_ZERO_COUNT, HIGH_NZC,
                    TIME_OF_DAY,
                    TIME_OF_WEEK
                    ));

    /**
     * The set of functions that require a fieldname
     */
    public static final Set<String> FIELD_NAME_FUNCTIONS =
            new HashSet<>(Arrays.asList(
                    DISTINCT_COUNT, DC,
                    LOW_DISTINCT_COUNT, LOW_DC,
                    HIGH_DISTINCT_COUNT, HIGH_DC,
                    INFO_CONTENT,
                    LOW_INFO_CONTENT,
                    HIGH_INFO_CONTENT,
                    METRIC,
                    MEAN, AVG,
                    HIGH_MEAN, HIGH_AVG,
                    LOW_MEAN, LOW_AVG,
                    MEDIAN,
                    MIN,
                    MAX,
                    SUM,
                    LOW_SUM,
                    HIGH_SUM,
                    NON_NULL_SUM,
                    LOW_NON_NULL_SUM,
                    HIGH_NON_NULL_SUM,
                    POPULATION_VARIANCE,
                    LOW_POPULATION_VARIANCE,
                    HIGH_POPULATION_VARIANCE,
                    LAT_LONG
                    ));

    /**
     * The set of functions that require a by fieldname
     */
    public static final Set<String> BY_FIELD_NAME_FUNCTIONS =
            new HashSet<>(Arrays.asList(
                    RARE,
                    FREQ_RARE
                    ));

    /**
     * The set of functions that require a over fieldname
     */
    public static final Set<String> OVER_FIELD_NAME_FUNCTIONS =
            new HashSet<>(Arrays.asList(
                    FREQ_RARE
                    ));

    /**
     * The set of functions that cannot have a by fieldname
     */
    public static final Set<String> NO_BY_FIELD_NAME_FUNCTIONS =
            new HashSet<>();

    /**
     * The set of functions that cannot have an over fieldname
     */
    public static final Set<String> NO_OVER_FIELD_NAME_FUNCTIONS =
            new HashSet<>(Arrays.asList(
                    NON_ZERO_COUNT, NZC,
                    LOW_NON_ZERO_COUNT, LOW_NZC,
                    HIGH_NON_ZERO_COUNT, HIGH_NZC
                    ));

    /**
     * The set of functions that must not be used with overlapping buckets
     */
    public static final Set<String> NO_OVERLAPPING_BUCKETS_FUNCTIONS =
            new HashSet<>(Arrays.asList(
                    RARE,
                    FREQ_RARE
                    ));

    /**
     * The set of functions that should not be used with overlapping buckets
     * as they gain no benefit but have overhead
     */
    public static final Set<String> OVERLAPPING_BUCKETS_FUNCTIONS_NOT_NEEDED =
            new HashSet<>(Arrays.asList(
                    MIN,
                    MAX,
                    TIME_OF_DAY,
                    TIME_OF_WEEK
                    ));

    /**
     * field names cannot contain any of these characters
     * ", \
     */
    public static final Character[] PROHIBITED_FIELDNAME_CHARACTERS = {'"', '\\'};
    public static final String PROHIBITED = String.join(",",
            Arrays.stream(PROHIBITED_FIELDNAME_CHARACTERS).map(
                    c -> Character.toString(c)).collect(Collectors.toList()));


    private final String detectorDescription;
    private final String function;
    private final String fieldName;
    private final String byFieldName;
    private final String overFieldName;
    private final String partitionFieldName;
    private final boolean useNull;
    private final ExcludeFrequent excludeFrequent;
    private final List<DetectionRule> detectorRules;

    public Detector(StreamInput in) throws IOException {
        detectorDescription = in.readString();
        function = in.readString();
        fieldName = in.readOptionalString();
        byFieldName = in.readOptionalString();
        overFieldName = in.readOptionalString();
        partitionFieldName = in.readOptionalString();
        useNull = in.readBoolean();
        excludeFrequent = in.readBoolean() ? ExcludeFrequent.readFromStream(in) : null;
        detectorRules = in.readList(DetectionRule::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(detectorDescription);
        out.writeString(function);
        out.writeOptionalString(fieldName);
        out.writeOptionalString(byFieldName);
        out.writeOptionalString(overFieldName);
        out.writeOptionalString(partitionFieldName);
        out.writeBoolean(useNull);
        if (excludeFrequent != null) {
            out.writeBoolean(true);
            excludeFrequent.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeList(detectorRules);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DETECTOR_DESCRIPTION_FIELD.getPreferredName(), detectorDescription);
        builder.field(FUNCTION_FIELD.getPreferredName(), function);
        if (fieldName != null) {
            builder.field(FIELD_NAME_FIELD.getPreferredName(), fieldName);
        }
        if (byFieldName != null) {
            builder.field(BY_FIELD_NAME_FIELD.getPreferredName(), byFieldName);
        }
        if (overFieldName != null) {
            builder.field(OVER_FIELD_NAME_FIELD.getPreferredName(), overFieldName);
        }
        if (partitionFieldName != null) {
            builder.field(PARTITION_FIELD_NAME_FIELD.getPreferredName(), partitionFieldName);
        }
        if (useNull) {
            builder.field(USE_NULL_FIELD.getPreferredName(), useNull);
        }
        if (excludeFrequent != null) {
            builder.field(EXCLUDE_FREQUENT_FIELD.getPreferredName(), excludeFrequent);
        }
        builder.field(DETECTOR_RULES_FIELD.getPreferredName(), detectorRules);
        builder.endObject();
        return builder;
    }

    private Detector(String detectorDescription, String function, String fieldName, String byFieldName, String overFieldName,
            String partitionFieldName, boolean useNull, ExcludeFrequent excludeFrequent, List<DetectionRule> detectorRules) {
        this.function = function;
        this.fieldName = fieldName;
        this.byFieldName = byFieldName;
        this.overFieldName = overFieldName;
        this.partitionFieldName = partitionFieldName;
        this.useNull = useNull;
        this.excludeFrequent = excludeFrequent;
        // REMOVE THIS LINE WHEN REMOVING JACKSON_DATABIND:
        detectorRules = detectorRules != null ? detectorRules : Collections.emptyList();
        this.detectorRules = Collections.unmodifiableList(detectorRules);
        this.detectorDescription = detectorDescription != null ? detectorDescription : DefaultDetectorDescription.of(this);
    }

    public String getDetectorDescription() {
        return detectorDescription;
    }

    /**
     * The analysis function used e.g. count, rare, min etc. There is no
     * validation to check this value is one a predefined set
     *
     * @return The function or <code>null</code> if not set
     */
    public String getFunction() {
        return function;
    }

    /**
     * The Analysis field
     *
     * @return The field to analyse
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * The 'by' field or <code>null</code> if not set.
     *
     * @return The 'by' field
     */
    public String getByFieldName() {
        return byFieldName;
    }

    /**
     * The 'over' field or <code>null</code> if not set.
     *
     * @return The 'over' field
     */
    public String getOverFieldName() {
        return overFieldName;
    }

    /**
     * Segments the analysis along another field to have completely
     * independent baselines for each instance of partitionfield
     *
     * @return The Partition Field
     */
    public String getPartitionFieldName() {
        return partitionFieldName;
    }

    /**
     * Where there isn't a value for the 'by' or 'over' field should a new
     * series be used as the 'null' series.
     *
     * @return true if the 'null' series should be created
     */
    public boolean isUseNull() {
        return useNull;
    }

    /**
     * Excludes frequently-occuring metrics from the analysis;
     * can apply to 'by' field, 'over' field, or both
     *
     * @return the value that the user set
     */
    public ExcludeFrequent getExcludeFrequent() {
        return excludeFrequent;
    }

    public List<DetectionRule> getDetectorRules() {
        return detectorRules;
    }

    /**
     * Returns a list with the byFieldName, overFieldName and partitionFieldName that are not null
     *
     * @return a list with the byFieldName, overFieldName and partitionFieldName that are not null
     */
    public List<String> extractAnalysisFields() {
        List<String> analysisFields = Arrays.asList(getByFieldName(),
                getOverFieldName(), getPartitionFieldName());
        return analysisFields.stream().filter(item -> item != null).collect(Collectors.toList());
    }

    public Set<String> extractReferencedFilters() {
        return detectorRules == null ? Collections.emptySet()
                : detectorRules.stream().map(DetectionRule::extractReferencedFilters)
                .flatMap(Set::stream).collect(Collectors.toSet());
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof Detector == false) {
            return false;
        }

        Detector that = (Detector) other;

        return Objects.equals(this.detectorDescription, that.detectorDescription) &&
                Objects.equals(this.function, that.function) &&
                Objects.equals(this.fieldName, that.fieldName) &&
                Objects.equals(this.byFieldName, that.byFieldName) &&
                Objects.equals(this.overFieldName, that.overFieldName) &&
                Objects.equals(this.partitionFieldName, that.partitionFieldName) &&
                Objects.equals(this.useNull, that.useNull) &&
                Objects.equals(this.excludeFrequent, that.excludeFrequent) &&
                Objects.equals(this.detectorRules, that.detectorRules);
    }

    @Override
    public int hashCode() {
        return Objects.hash(detectorDescription, function, fieldName, byFieldName,
                overFieldName, partitionFieldName, useNull, excludeFrequent,
                detectorRules);
    }

    public static class Builder {

        /**
         * Functions that do not support rules:
         * <ul>
         * <li>lat_long - because it is a multivariate feature
         * <li>metric - because having the same conditions on min,max,mean is
         * error-prone
         * </ul>
         */
        static final Set<String> FUNCTIONS_WITHOUT_RULE_SUPPORT = new HashSet<>(Arrays.asList(Detector.LAT_LONG, Detector.METRIC));

        private String detectorDescription;
        private String function;
        private String fieldName;
        private String byFieldName;
        private String overFieldName;
        private String partitionFieldName;
        private boolean useNull = false;
        private ExcludeFrequent excludeFrequent;
        private List<DetectionRule> detectorRules = Collections.emptyList();

        public Builder() {
        }

        public Builder(Detector detector) {
            detectorDescription = detector.detectorDescription;
            function = detector.function;
            fieldName = detector.fieldName;
            byFieldName = detector.byFieldName;
            overFieldName = detector.overFieldName;
            partitionFieldName = detector.partitionFieldName;
            useNull = detector.useNull;
            excludeFrequent = detector.excludeFrequent;
            detectorRules = new ArrayList<>(detector.detectorRules.size());
            for (DetectionRule rule : detector.getDetectorRules()) {
                detectorRules.add(rule);
            }
        }

        public Builder(String function, String fieldName) {
            this.function = function;
            this.fieldName = fieldName;
        }

        public void setDetectorDescription(String detectorDescription) {
            this.detectorDescription = detectorDescription;
        }

        public void setFunction(String function) {
            this.function = function;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public void setByFieldName(String byFieldName) {
            this.byFieldName = byFieldName;
        }

        public void setOverFieldName(String overFieldName) {
            this.overFieldName = overFieldName;
        }

        public void setPartitionFieldName(String partitionFieldName) {
            this.partitionFieldName = partitionFieldName;
        }

        public void setUseNull(boolean useNull) {
            this.useNull = useNull;
        }

        public void setExcludeFrequent(ExcludeFrequent excludeFrequent) {
            this.excludeFrequent = excludeFrequent;
        }

        public void setDetectorRules(List<DetectionRule> detectorRules) {
            this.detectorRules = detectorRules;
        }

        public List<DetectionRule> getDetectorRules() {
            return detectorRules;
        }

        public Detector build() {
            return build(false);
        }

        public Detector build(boolean isSummarised) {
            boolean emptyField = Strings.isEmpty(fieldName);
            boolean emptyByField = Strings.isEmpty(byFieldName);
            boolean emptyOverField = Strings.isEmpty(overFieldName);
            if (Detector.ANALYSIS_FUNCTIONS.contains(function) == false) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_UNKNOWN_FUNCTION, function));
            }

            if (emptyField && emptyByField && emptyOverField) {
                if (!Detector.COUNT_WITHOUT_FIELD_FUNCTIONS.contains(function)) {
                    throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_NO_ANALYSIS_FIELD_NOT_COUNT));
                }
            }

            if (isSummarised && Detector.METRIC.equals(function)) {
                throw new IllegalArgumentException(
                        Messages.getMessage(Messages.JOB_CONFIG_FUNCTION_INCOMPATIBLE_PRESUMMARIZED, Detector.METRIC));
            }

            // check functions have required fields

            if (emptyField && Detector.FIELD_NAME_FUNCTIONS.contains(function)) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_FUNCTION_REQUIRES_FIELDNAME, function));
            }

            if (!emptyField && (Detector.FIELD_NAME_FUNCTIONS.contains(function) == false)) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_FIELDNAME_INCOMPATIBLE_FUNCTION, function));
            }

            if (emptyByField && Detector.BY_FIELD_NAME_FUNCTIONS.contains(function)) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_FUNCTION_REQUIRES_BYFIELD, function));
            }

            if (!emptyByField && Detector.NO_BY_FIELD_NAME_FUNCTIONS.contains(function)) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_BYFIELD_INCOMPATIBLE_FUNCTION, function));
            }

            if (emptyOverField && Detector.OVER_FIELD_NAME_FUNCTIONS.contains(function)) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_FUNCTION_REQUIRES_OVERFIELD, function));
            }

            if (!emptyOverField && Detector.NO_OVER_FIELD_NAME_FUNCTIONS.contains(function)) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_OVERFIELD_INCOMPATIBLE_FUNCTION, function));
            }

            // field names cannot contain certain characters
            String[] fields = { fieldName, byFieldName, overFieldName, partitionFieldName };
            for (String field : fields) {
                verifyFieldName(field);
            }

            String function = this.function == null ? Detector.METRIC : this.function;
            if (detectorRules.isEmpty() == false) {
                if (FUNCTIONS_WITHOUT_RULE_SUPPORT.contains(function)) {
                    String msg = Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_NOT_SUPPORTED_BY_FUNCTION, function);
                    throw new IllegalArgumentException(msg);
                }
                for (DetectionRule rule : detectorRules) {
                    checkScoping(rule);
                }
            }

            return new Detector(detectorDescription, function, fieldName, byFieldName, overFieldName, partitionFieldName,
                    useNull, excludeFrequent, detectorRules);
        }

        public List<String> extractAnalysisFields() {
            List<String> analysisFields = Arrays.asList(byFieldName,
                    overFieldName, partitionFieldName);
            return analysisFields.stream().filter(item -> item != null).collect(Collectors.toList());
        }

        /**
         * Check that the characters used in a field name will not cause problems.
         *
         * @param field
         *            The field name to be validated
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

        private void checkScoping(DetectionRule rule) throws ElasticsearchParseException {
            String targetFieldName = rule.getTargetFieldName();
            checkTargetFieldNameIsValid(extractAnalysisFields(), targetFieldName);
            List<String> validOptions = getValidFieldNameOptions(rule);
            for (RuleCondition condition : rule.getRuleConditions()) {
                if (!validOptions.contains(condition.getFieldName())) {
                    String msg = Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_CONDITION_INVALID_FIELD_NAME, validOptions,
                            condition.getFieldName());
                    throw new IllegalArgumentException(msg);
                }
            }
        }

        private void checkTargetFieldNameIsValid(List<String> analysisFields, String targetFieldName)
                throws ElasticsearchParseException {
            if (targetFieldName != null && !analysisFields.contains(targetFieldName)) {
                String msg =
                        Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_INVALID_TARGET_FIELD_NAME, analysisFields, targetFieldName);
                throw new IllegalArgumentException(msg);
            }
        }

        private List<String> getValidFieldNameOptions(DetectionRule rule) {
            List<String> result = new ArrayList<>();
            if (overFieldName != null) {
                result.add(byFieldName == null ? overFieldName : byFieldName);
            } else if (byFieldName != null) {
                result.add(byFieldName);
            }

            if (rule.getTargetFieldName() != null) {
                ScopingLevel targetLevel = ScopingLevel.from(this, rule.getTargetFieldName());
                result = result.stream().filter(field -> targetLevel.isHigherThan(ScopingLevel.from(this, field)))
                        .collect(Collectors.toList());
            }

            if (isEmptyFieldNameAllowed(rule)) {
                result.add(null);
            }
            return result;
        }

        private boolean isEmptyFieldNameAllowed(DetectionRule rule) {
            List<String> analysisFields = extractAnalysisFields();
            return analysisFields.isEmpty() || (rule.getTargetFieldName() != null && analysisFields.size() == 1);
        }

        enum ScopingLevel {
            PARTITION(3),
            OVER(2),
            BY(1);

            int level;

            ScopingLevel(int level) {
                this.level = level;
            }

            boolean isHigherThan(ScopingLevel other) {
                return level > other.level;
            }

            static ScopingLevel from(Detector.Builder detector, String fieldName) {
                if (fieldName.equals(detector.partitionFieldName)) {
                    return ScopingLevel.PARTITION;
                }
                if (fieldName.equals(detector.overFieldName)) {
                    return ScopingLevel.OVER;
                }
                if (fieldName.equals(detector.byFieldName)) {
                    return ScopingLevel.BY;
                }
                throw new IllegalArgumentException(
                        "fieldName '" + fieldName + "' does not match an analysis field");
            }
        }

    }
}
