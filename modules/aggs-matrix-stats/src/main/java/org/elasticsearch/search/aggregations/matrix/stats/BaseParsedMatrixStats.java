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
package org.elasticsearch.search.aggregations.matrix.stats;

import org.elasticsearch.search.aggregations.ParsedAggregation;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class BaseParsedMatrixStats extends ParsedAggregation implements MatrixStats {
    protected final Map<String, Long> counts = new LinkedHashMap<>();
    protected final Map<String, Double> means = new HashMap<>();
    protected final Map<String, Double> variances = new HashMap<>();
    protected final Map<String, Double> skewness = new HashMap<>();
    protected final Map<String, Double> kurtosis = new HashMap<>();
    protected final Map<String, Map<String, Double>> covariances = new HashMap<>();
    protected final Map<String, Map<String, Double>> correlations = new HashMap<>();

    protected long docCount;

    protected void setDocCount(long docCount) {
        this.docCount = docCount;
    }

    @Override
    public long getDocCount() {
        return docCount;
    }

    @Override
    public long getFieldCount(String field) {
        if (counts.containsKey(field) == false) {
            return 0;
        }
        return counts.get(field);
    }

    @Override
    public double getMean(String field) {
        return checkedGet(means, field);
    }

    @Override
    public double getVariance(String field) {
        return checkedGet(variances, field);
    }

    @Override
    public double getSkewness(String field) {
        return checkedGet(skewness, field);
    }

    @Override
    public double getKurtosis(String field) {
        return checkedGet(kurtosis, field);
    }

    @Override
    public double getCovariance(String fieldX, String fieldY) {
        if (fieldX.equals(fieldY)) {
            return checkedGet(variances, fieldX);
        }
        return MatrixStatsResults.getValFromUpperTriangularMatrix(covariances, fieldX, fieldY);
    }

    @Override
    public double getCorrelation(String fieldX, String fieldY) {
        if (fieldX.equals(fieldY)) {
            return 1.0;
        }
        return MatrixStatsResults.getValFromUpperTriangularMatrix(correlations, fieldX, fieldY);
    }

    protected static <T> T checkedGet(final Map<String, T> values, final String fieldName) {
        if (fieldName == null) {
            throw new IllegalArgumentException("field name cannot be null");
        }
        if (values.containsKey(fieldName) == false) {
            throw new IllegalArgumentException("field " + fieldName + " does not exist");
        }
        return values.get(fieldName);
    }

}
