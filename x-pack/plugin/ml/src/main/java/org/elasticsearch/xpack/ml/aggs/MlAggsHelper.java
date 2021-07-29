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
import org.elasticsearch.search.aggregations.bucket.InternalSingleBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class MlAggsHelper {

    private MlAggsHelper() {}

    public static InvalidAggregationPathException invalidPathException(List<String> path, String aggType, String aggName) {
        return new InvalidAggregationPathException("unknown property " + path + " for " + aggType + " aggregation [" + aggName + "]");
    }

    public static boolean pathElementContainsBucketKey(AggregationPath.PathElement pathElement) {
        return pathElement.key != null && pathElement.key.startsWith("'") && pathElement.key.endsWith("'");
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
        List<AggregationPath.PathElement> parsedPath = AggregationPath.parse(bucketPath).getPathElements();
        for (Aggregation aggregation : aggregations) {
            int currElem = 0;
            if (aggregation.getName().equals(parsedPath.get(0).name)) {
                Aggregation currentAgg = aggregation;
                while (currElem < parsedPath.size() - 1) {
                    if (currentAgg instanceof InternalSingleBucketAggregation) {
                        ++currElem;
                        currentAgg = ((InternalSingleBucketAggregation) currentAgg).getAggregations().get(parsedPath.get(currElem).name);
                    } else if (pathElementContainsBucketKey(parsedPath.get(currElem))) {
                        if ((currentAgg instanceof InternalMultiBucketAggregation) == false) {
                            throw new AggregationExecutionException(
                                "bucket_path ["
                                    + bucketPath
                                    + "] indicates bucket_key ["
                                    + parsedPath.get(currElem).key
                                    + "] at position ["
                                    + currElem
                                    + "] but encountered on agg ["
                                    + Optional.ofNullable(currentAgg).map(Aggregation::getName).orElse("__missing__")
                                    + "] which is not a multi_bucket aggregation"
                            );
                        }
                        InternalMultiBucketAggregation.InternalBucket bucket =
                            (InternalMultiBucketAggregation.InternalBucket) ((InternalMultiBucketAggregation<?, ?>) currentAgg).getProperty(
                                parsedPath.get(currElem).key
                            );
                        if (bucket == null) {
                            throw new AggregationExecutionException(
                                "missing bucket ["
                                    + parsedPath.get(currElem).key
                                    + "] for agg ["
                                    + currentAgg.getName()
                                    + "] while extracting bucket path ["
                                    + bucketPath
                                    + "]"
                            );
                        }
                        if (currElem == parsedPath.size() - 1) {
                            throw new AggregationExecutionException("invalid bucket path ends at [" + parsedPath.get(currElem).key + "]");
                        }
                        Aggregations innerAggs = bucket.getAggregations();
                        ++currElem;
                        currentAgg = innerAggs.get(parsedPath.get(currElem).name);
                    } else {
                        break;
                    }
                }
                if (currentAgg instanceof InternalMultiBucketAggregation == false) {
                    String msg = currentAgg == null
                        ? "did not find multi-bucket aggregation for extraction."
                        : "did not find multi-bucket aggregation for extraction. Found [" + currentAgg.getName() + "]";
                    throw new AggregationExecutionException(msg);
                }
                List<String> sublistedPath = parsedPath.subList(currElem, parsedPath.size())
                    .stream()
                    .flatMap(p -> p.key != null ? Stream.of(p.name, p.key) : Stream.of(p.name))
                    .collect(Collectors.toList());
                // First element is the current agg, so we want the rest of the path
                sublistedPath = sublistedPath.subList(1, sublistedPath.size());
                InternalMultiBucketAggregation<?, ?> multiBucketsAgg = (InternalMultiBucketAggregation<?, ?>) currentAgg;
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
                return Optional.of(
                    new DoubleBucketValues(
                        docCounts.stream().mapToLong(Long::longValue).toArray(),
                        values.stream().mapToDouble(Double::doubleValue).toArray()
                    )
                );
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
