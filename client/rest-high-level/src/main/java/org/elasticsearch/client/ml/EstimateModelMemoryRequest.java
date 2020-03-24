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

package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.client.ml.job.config.AnalysisConfig;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Request to estimate the model memory an analysis config is likely to need given supplied field cardinalities.
 */
public class EstimateModelMemoryRequest implements Validatable, ToXContentObject {

    public static final String ANALYSIS_CONFIG = "analysis_config";
    public static final String OVERALL_CARDINALITY = "overall_cardinality";
    public static final String MAX_BUCKET_CARDINALITY = "max_bucket_cardinality";

    private final AnalysisConfig analysisConfig;
    private Map<String, Long> overallCardinality = Collections.emptyMap();
    private Map<String, Long> maxBucketCardinality = Collections.emptyMap();

    @Override
    public Optional<ValidationException> validate() {
        return Optional.empty();
    }

    public EstimateModelMemoryRequest(AnalysisConfig analysisConfig) {
        this.analysisConfig = Objects.requireNonNull(analysisConfig);
    }

    public AnalysisConfig getAnalysisConfig() {
        return analysisConfig;
    }

    public Map<String, Long> getOverallCardinality() {
        return overallCardinality;
    }

    public void setOverallCardinality(Map<String, Long> overallCardinality) {
        this.overallCardinality = Collections.unmodifiableMap(overallCardinality);
    }

    public Map<String, Long> getMaxBucketCardinality() {
        return maxBucketCardinality;
    }

    public void setMaxBucketCardinality(Map<String, Long> maxBucketCardinality) {
        this.maxBucketCardinality = Collections.unmodifiableMap(maxBucketCardinality);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ANALYSIS_CONFIG, analysisConfig);
        if (overallCardinality.isEmpty() == false) {
            builder.field(OVERALL_CARDINALITY, overallCardinality);
        }
        if (maxBucketCardinality.isEmpty() == false) {
            builder.field(MAX_BUCKET_CARDINALITY, maxBucketCardinality);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(analysisConfig, overallCardinality, maxBucketCardinality);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        EstimateModelMemoryRequest that = (EstimateModelMemoryRequest) other;
        return Objects.equals(analysisConfig, that.analysisConfig) &&
            Objects.equals(overallCardinality, that.overallCardinality) &&
            Objects.equals(maxBucketCardinality, that.maxBucketCardinality);
    }
}
