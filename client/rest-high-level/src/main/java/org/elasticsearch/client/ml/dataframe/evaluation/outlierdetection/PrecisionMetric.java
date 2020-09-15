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
package org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection;

import org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class PrecisionMetric extends AbstractConfusionMatrixMetric {

    public static final String NAME = "precision";

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<PrecisionMetric, Void> PARSER =
        new ConstructingObjectParser<>(NAME, args -> new PrecisionMetric((List<Double>) args[0]));

    static {
        PARSER.declareDoubleArray(constructorArg(), AT);
    }

    public static PrecisionMetric fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static PrecisionMetric at(Double... at) {
        return new PrecisionMetric(Arrays.asList(at));
    }

    public PrecisionMetric(List<Double> at) {
        super(at);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PrecisionMetric that = (PrecisionMetric) o;
        return Arrays.equals(thresholds, that.thresholds);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(thresholds);
    }

    public static class Result implements EvaluationMetric.Result {

        public static Result fromXContent(XContentParser parser) throws IOException {
            return new Result(parser.map(LinkedHashMap::new, p -> p.doubleValue()));
        }

        private final Map<String, Double> results;

        public Result(Map<String, Double> results) {
            this.results = Objects.requireNonNull(results);
        }

        @Override
        public String getMetricName() {
            return NAME;
        }

        public Double getScoreByThreshold(String threshold) {
            return results.get(threshold);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            return builder.map(results);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result that = (Result) o;
            return Objects.equals(results, that.results);
        }

        @Override
        public int hashCode() {
            return Objects.hash(results);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
