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
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Defines the fields and functions used in the analysis. A combination of <code>field_name</code>,
 * <code>by_field_name</code> and <code>over_field_name</code> can be used depending on the specific
 * function chosen. For more information see the
 * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-put-job.html">create anomaly detection
 * jobs API</a> and <a href="https://www.elastic.co/guide/en/elastic-stack-overview/current/ml-functions.html">detector functions</a>.
 */
public class Detector implements ToXContentObject {

    public enum ExcludeFrequent {
        ALL,
        NONE,
        BY,
        OVER;

        /**
         * Case-insensitive from string method.
         * Works with either ALL, All, etc.
         *
         * @param value String representation
         * @return The data format
         */
        public static ExcludeFrequent forString(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
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
    public static final ParseField CUSTOM_RULES_FIELD = new ParseField("custom_rules");
    public static final ParseField DETECTOR_INDEX = new ParseField("detector_index");

    public static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("detector", true, Builder::new);

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
        PARSER.declareObjectArray(Builder::setRules, (p, c) -> DetectionRule.PARSER.apply(p, c).build(), CUSTOM_RULES_FIELD);
        PARSER.declareInt(Builder::setDetectorIndex, DETECTOR_INDEX);
    }

    private final String detectorDescription;
    private final DetectorFunction function;
    private final String fieldName;
    private final String byFieldName;
    private final String overFieldName;
    private final String partitionFieldName;
    private final boolean useNull;
    private final ExcludeFrequent excludeFrequent;
    private final List<DetectionRule> rules;
    private final int detectorIndex;

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
        if (rules.isEmpty() == false) {
            builder.field(CUSTOM_RULES_FIELD.getPreferredName(), rules);
        }
        // negative means unknown
        if (detectorIndex >= 0) {
            builder.field(DETECTOR_INDEX.getPreferredName(), detectorIndex);
        }
        builder.endObject();
        return builder;
    }

    private Detector(String detectorDescription, DetectorFunction function, String fieldName, String byFieldName, String overFieldName,
                     String partitionFieldName, boolean useNull, ExcludeFrequent excludeFrequent, List<DetectionRule> rules,
                     int detectorIndex) {
        this.function = function;
        this.fieldName = fieldName;
        this.byFieldName = byFieldName;
        this.overFieldName = overFieldName;
        this.partitionFieldName = partitionFieldName;
        this.useNull = useNull;
        this.excludeFrequent = excludeFrequent;
        this.rules = Collections.unmodifiableList(rules);
        this.detectorDescription = detectorDescription != null ? detectorDescription : DefaultDetectorDescription.of(this);
        this.detectorIndex = detectorIndex;
    }

    public String getDetectorDescription() {
        return detectorDescription;
    }

    /**
     * The analysis function used e.g. count, rare, min etc.
     *
     * @return The function or <code>null</code> if not set
     */
    public DetectorFunction getFunction() {
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
     * Excludes frequently-occurring metrics from the analysis;
     * can apply to 'by' field, 'over' field, or both
     *
     * @return the value that the user set
     */
    public ExcludeFrequent getExcludeFrequent() {
        return excludeFrequent;
    }

    public List<DetectionRule> getRules() {
        return rules;
    }

    /**
     * @return the detector index or a negative number if unknown
     */
    public int getDetectorIndex() {
        return detectorIndex;
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
                Objects.equals(this.rules, that.rules) &&
                this.detectorIndex == that.detectorIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(detectorDescription, function, fieldName, byFieldName, overFieldName, partitionFieldName, useNull,
                excludeFrequent, rules, detectorIndex);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String detectorDescription;
        private DetectorFunction function;
        private String fieldName;
        private String byFieldName;
        private String overFieldName;
        private String partitionFieldName;
        private boolean useNull = false;
        private ExcludeFrequent excludeFrequent;
        private List<DetectionRule> rules = Collections.emptyList();
        // negative means unknown
        private int detectorIndex = -1;

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
            rules = new ArrayList<>(detector.rules);
            detectorIndex = detector.detectorIndex;
        }

        public Builder(String function, String fieldName) {
            this(DetectorFunction.fromString(function), fieldName);
        }

        public Builder(DetectorFunction function, String fieldName) {
            this.function = function;
            this.fieldName = fieldName;
        }

        public Builder setDetectorDescription(String detectorDescription) {
            this.detectorDescription = detectorDescription;
            return this;
        }

        public Builder setFunction(String function) {
            this.function = DetectorFunction.fromString(function);
            return this;
        }

        public Builder setFieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Builder setByFieldName(String byFieldName) {
            this.byFieldName = byFieldName;
            return this;
        }

        public Builder setOverFieldName(String overFieldName) {
            this.overFieldName = overFieldName;
            return this;
        }

        public Builder setPartitionFieldName(String partitionFieldName) {
            this.partitionFieldName = partitionFieldName;
            return this;
        }

        public Builder setUseNull(boolean useNull) {
            this.useNull = useNull;
            return this;
        }

        public Builder setExcludeFrequent(ExcludeFrequent excludeFrequent) {
            this.excludeFrequent = excludeFrequent;
            return this;
        }

        public Builder setRules(List<DetectionRule> rules) {
            this.rules = rules;
            return this;
        }

        public Builder setDetectorIndex(int detectorIndex) {
            this.detectorIndex = detectorIndex;
            return this;
        }

        public Detector build() {
            return new Detector(detectorDescription, function, fieldName, byFieldName, overFieldName, partitionFieldName,
                useNull, excludeFrequent, rules, detectorIndex);
        }
    }
}
