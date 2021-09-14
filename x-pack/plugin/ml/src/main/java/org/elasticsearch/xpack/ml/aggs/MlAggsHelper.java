/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InvalidAggregationPathException;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class MlAggsHelper {

    private MlAggsHelper() { }

    public static InvalidAggregationPathException invalidPathException(List<String> path, String aggType, String aggName) {
        return new InvalidAggregationPathException(
            "unknown property " + path + " for " + aggType + " aggregation [" + aggName + "]"
        );
    }

    /**
     * This extracts the bucket values as doubles from the passed aggregations.
     *
     * The gap policy is always `INSERT_ZERO`
     * @param bucketPath The bucket path from which to extract values
     * @param aggregations The aggregations
     * @return The double values and doc_counts extracted from the path if the bucket path exists and the value is a valid number
     */
    public static Optional<DoubleBucketValues> extractDoubleBucketedValues(String bucketPath, Aggregations aggregations) {
        List<String> parsedPath = AggregationPath.parse(bucketPath).getPathElementsAsStringList();
        for (Aggregation aggregation : aggregations) {
            if (aggregation.getName().equals(parsedPath.get(0))) {
                List<String> sublistedPath = parsedPath.subList(1, parsedPath.size());
                InternalMultiBucketAggregation<?, ?> multiBucketsAgg = (InternalMultiBucketAggregation<?, ?>) aggregation;
                List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = multiBucketsAgg.getBuckets();
                List<Double> values = new ArrayList<>(buckets.size());
                List<Long> docCounts = new ArrayList<>(buckets.size());
                for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
                    Double bucketValue = BucketHelpers.resolveBucketValue(
                        multiBucketsAgg,
                        bucket,
                        sublistedPath,
                        BucketHelpers.GapPolicy.INSERT_ZEROS
                    );
                    if (bucketValue == null || Double.isNaN(bucketValue)) {
                        throw new AggregationExecutionException(
                            "missing or invalid bucket value found for path ["
                                + bucketPath
                                + "] in bucket ["
                                + bucket.getKeyAsString()
                                + "]"
                        );
                    }
                    values.add(bucketValue);
                    docCounts.add(bucket.getDocCount());
                }
                return Optional.of(new DoubleBucketValues(
                    docCounts.stream().mapToLong(Long::longValue).toArray(),
                    values.stream().mapToDouble(Double::doubleValue).toArray()
                ));
            }
        }
        return Optional.empty();
    }

    /**
     * Utility class for holding an unboxed double value and the document count for a bucket
     */
    public static class DoubleBucketValues {
        private final long[] docCounts;
        private final double[] values;

        public DoubleBucketValues(long[] docCounts, double[] values) {
            this.docCounts = docCounts;
            this.values = values;
        }

        public long[] getDocCounts() {
            return docCounts;
        }

        public double[] getValues() {
            return values;
        }
    }

}
