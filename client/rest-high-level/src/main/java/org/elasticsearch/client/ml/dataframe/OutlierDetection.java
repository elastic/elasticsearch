/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class OutlierDetection implements DataFrameAnalysis {

    public static OutlierDetection fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null).build();
    }

    public static OutlierDetection createDefault() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final ParseField NAME = new ParseField("outlier_detection");
    static final ParseField N_NEIGHBORS = new ParseField("n_neighbors");
    static final ParseField METHOD = new ParseField("method");
    public static final ParseField FEATURE_INFLUENCE_THRESHOLD = new ParseField("feature_influence_threshold");
    static final ParseField COMPUTE_FEATURE_INFLUENCE = new ParseField("compute_feature_influence");
    static final ParseField OUTLIER_FRACTION = new ParseField("outlier_fraction");
    static final ParseField STANDARDIZATION_ENABLED = new ParseField("standardization_enabled");

    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(NAME.getPreferredName(), true, Builder::new);

    static {
        PARSER.declareInt(Builder::setNNeighbors, N_NEIGHBORS);
        PARSER.declareString(Builder::setMethod, Method::fromString, METHOD);
        PARSER.declareDouble(Builder::setFeatureInfluenceThreshold, FEATURE_INFLUENCE_THRESHOLD);
        PARSER.declareBoolean(Builder::setComputeFeatureInfluence, COMPUTE_FEATURE_INFLUENCE);
        PARSER.declareDouble(Builder::setOutlierFraction, OUTLIER_FRACTION);
        PARSER.declareBoolean(Builder::setStandardizationEnabled, STANDARDIZATION_ENABLED);
    }

    /**
     * The number of neighbors. Leave unspecified for dynamic detection.
     */
    private final Integer nNeighbors;

    /**
     * The method. Leave unspecified for a dynamic mixture of methods.
     */
    private final Method method;

    /**
     * The min outlier score required to calculate feature influence. Defaults to 0.1.
     */
    private final Double featureInfluenceThreshold;

    /**
     * Whether to compute feature influence or not. Defaults to true.
     */
    private final Boolean computeFeatureInfluence;

    /**
     * The proportion of data assumed to be outlying prior to outlier detection. Defaults to 0.05.
     */
    private final Double outlierFraction;

    /**
     * Whether to perform standardization.
     */
    private final Boolean standardizationEnabled;

    private OutlierDetection(Integer nNeighbors, Method method, Double featureInfluenceThreshold, Boolean computeFeatureInfluence,
                             Double outlierFraction, Boolean standardizationEnabled) {
        this.nNeighbors = nNeighbors;
        this.method = method;
        this.featureInfluenceThreshold = featureInfluenceThreshold;
        this.computeFeatureInfluence = computeFeatureInfluence;
        this.outlierFraction = outlierFraction;
        this.standardizationEnabled = standardizationEnabled;
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    public Integer getNNeighbors() {
        return nNeighbors;
    }

    public Method getMethod() {
        return method;
    }

    public Double getFeatureInfluenceThreshold() {
        return featureInfluenceThreshold;
    }

    public Boolean getComputeFeatureInfluence() {
        return computeFeatureInfluence;
    }

    public Double getOutlierFraction() {
        return outlierFraction;
    }

    public Boolean getStandardizationEnabled() {
        return standardizationEnabled;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (nNeighbors != null) {
            builder.field(N_NEIGHBORS.getPreferredName(), nNeighbors);
        }
        if (method != null) {
            builder.field(METHOD.getPreferredName(), method);
        }
        if (featureInfluenceThreshold != null) {
            builder.field(FEATURE_INFLUENCE_THRESHOLD.getPreferredName(), featureInfluenceThreshold);
        }
        if (computeFeatureInfluence != null) {
            builder.field(COMPUTE_FEATURE_INFLUENCE.getPreferredName(), computeFeatureInfluence);
        }
        if (outlierFraction != null) {
            builder.field(OUTLIER_FRACTION.getPreferredName(), outlierFraction);
        }
        if (standardizationEnabled != null) {
            builder.field(STANDARDIZATION_ENABLED.getPreferredName(), standardizationEnabled);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OutlierDetection other = (OutlierDetection) o;
        return Objects.equals(nNeighbors, other.nNeighbors)
            && Objects.equals(method, other.method)
            && Objects.equals(featureInfluenceThreshold, other.featureInfluenceThreshold)
            && Objects.equals(computeFeatureInfluence, other.computeFeatureInfluence)
            && Objects.equals(outlierFraction, other.outlierFraction)
            && Objects.equals(standardizationEnabled, other.standardizationEnabled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nNeighbors, method, featureInfluenceThreshold, computeFeatureInfluence, outlierFraction,
            standardizationEnabled);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public enum Method {
        LOF, LDOF, DISTANCE_KTH_NN, DISTANCE_KNN;

        public static Method fromString(String value) {
            return Method.valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static class Builder {

        private Integer nNeighbors;
        private Method method;
        private Double featureInfluenceThreshold;
        private Boolean computeFeatureInfluence;
        private Double outlierFraction;
        private Boolean standardizationEnabled;

        private Builder() {}

        public Builder setNNeighbors(Integer nNeighbors) {
            this.nNeighbors = nNeighbors;
            return this;
        }

        public Builder setMethod(Method method) {
            this.method = method;
            return this;
        }

        public Builder setFeatureInfluenceThreshold(Double featureInfluenceThreshold) {
            this.featureInfluenceThreshold = featureInfluenceThreshold;
            return this;
        }

        public Builder setComputeFeatureInfluence(Boolean computeFeatureInfluence) {
            this.computeFeatureInfluence = computeFeatureInfluence;
            return this;
        }

        public Builder setOutlierFraction(Double outlierFraction) {
            this.outlierFraction = outlierFraction;
            return this;
        }

        public Builder setStandardizationEnabled(Boolean standardizationEnabled) {
            this.standardizationEnabled = standardizationEnabled;
            return this;
        }

        public OutlierDetection build() {
            return new OutlierDetection(nNeighbors, method, featureInfluenceThreshold, computeFeatureInfluence, outlierFraction,
                standardizationEnabled);
        }
    }
}
