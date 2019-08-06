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

import org.elasticsearch.common.Nullable;
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

    private static ObjectParser<Builder, Void> PARSER = new ObjectParser<>(NAME.getPreferredName(), true, Builder::new);

    static {
        PARSER.declareInt(Builder::setNNeighbors, N_NEIGHBORS);
        PARSER.declareField(Builder::setMethod, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return Method.fromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, METHOD, ObjectParser.ValueType.STRING);
        PARSER.declareDouble(Builder::setFeatureInfluenceThreshold, FEATURE_INFLUENCE_THRESHOLD);
    }

    private final Integer nNeighbors;
    private final Method method;
    private final Double featureInfluenceThreshold;

    /**
     * Constructs the outlier detection configuration
     * @param nNeighbors The number of neighbors. Leave unspecified for dynamic detection.
     * @param method The method. Leave unspecified for a dynamic mixture of methods.
     * @param featureInfluenceThreshold The min outlier score required to calculate feature influence. Defaults to 0.1.
     */
    private OutlierDetection(@Nullable Integer nNeighbors, @Nullable Method method, @Nullable Double featureInfluenceThreshold) {
        this.nNeighbors = nNeighbors;
        this.method = method;
        this.featureInfluenceThreshold = featureInfluenceThreshold;
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
            && Objects.equals(featureInfluenceThreshold, other.featureInfluenceThreshold);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nNeighbors, method, featureInfluenceThreshold);
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

        public OutlierDetection build() {
            return new OutlierDetection(nNeighbors, method, featureInfluenceThreshold);
        }
    }
}
