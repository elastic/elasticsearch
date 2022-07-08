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

public final class MlAggsHelper {

    private MlAggsHelper() {}

    public static InvalidAggregationPathException invalidPathException(List<String> path, String aggType, String aggName) {
        return new InvalidAggregationPathException("unknown property " + path + " for " + aggType + " aggregation [" + aggName + "]");
    }

    public static boolean pathElementContainsBucketKey(AggregationPath.PathElement pathElement) {
        return pathElement.key() != null && pathElement.key().startsWith("'") && pathElement.key().endsWith("'");
    }

    /**
     * This extracts the bucket values as doubles from the passed aggregations.
     *
     * The gap policy is always `INSERT_ZEROS`
     * @param bucketPath The bucket path from which to extract values
     * @param aggregations The aggregations
     * @return The double values and doc_counts extracted from the path if the bucket path exists and the value is a valid number
     */
    public static Optional<DoubleBucketValues> extractDoubleBucketedValues(String bucketPath, Aggregations aggregations) {
        return extractDoubleBucketedValues(bucketPath, aggregations, BucketHelpers.GapPolicy.INSERT_ZEROS, false);
    }

    /**
     * This extracts the bucket values as doubles.
     *
     * If the gap policy is skippable, the true bucket index (as stored in the aggregation) is returned as well.
     * @param bucketPath The bucket path from which to extract values
     * @param aggregations The aggregations
     * @param gapPolicy the desired gap policy
     * @param excludeLastBucket should the last bucket be excluded? This is useful when excluding potentially partial buckets
     * @return The double values, doc_counts, and bucket index positions extracted from the path if the bucket path exists
     */
    public static Optional<DoubleBucketValues> extractDoubleBucketedValues(
        String bucketPath,
        Aggregations aggregations,
        BucketHelpers.GapPolicy gapPolicy,
        boolean excludeLastBucket
    ) {
        List<AggregationPath.PathElement> parsedPath = AggregationPath.parse(bucketPath).getPathElements();
        for (Aggregation aggregation : aggregations) {
            // Now that we have found the first agg in the path, resolve to the first non-qualified multi-bucket path
            if (aggregation.getName().equals(parsedPath.get(0).name())) {
                int currElement = 0;
                Aggregation currentAgg = aggregation;
                while (currElement < parsedPath.size() - 1) {
                    if (currentAgg == null) {
                        throw new IllegalArgumentException(
                            "bucket_path ["
                                + bucketPath
                                + "] expected aggregation with name ["
                                + parsedPath.get(currElement).name()
                                + "] but was missing in search response"
                        );
                    }
                    if (currentAgg instanceof InternalSingleBucketAggregation singleBucketAggregation) {
                        currentAgg = singleBucketAggregation.getAggregations().get(parsedPath.get(++currElement).name());
                    } else if (pathElementContainsBucketKey(parsedPath.get(currElement))) {
                        if (currentAgg instanceof InternalMultiBucketAggregation<?, ?> multiBucketAggregation) {
                            InternalMultiBucketAggregation.InternalBucket bucket =
                                (InternalMultiBucketAggregation.InternalBucket) multiBucketAggregation.getProperty(
                                    parsedPath.get(currElement).key()
                                );
                            if (bucket == null) {
                                throw new AggregationExecutionException(
                                    "missing bucket ["
                                        + parsedPath.get(currElement).key()
                                        + "] for agg ["
                                        + currentAgg.getName()
                                        + "] while extracting bucket path ["
                                        + bucketPath
                                        + "]"
                                );
                            }
                            if (currElement == parsedPath.size() - 1) {
                                throw new AggregationExecutionException(
                                    "invalid bucket path ends at [" + parsedPath.get(currElement).key() + "]"
                                );
                            }
                            currentAgg = bucket.getAggregations().get(parsedPath.get(++currElement).name());
                        } else {
                            throw new AggregationExecutionException(
                                "bucket_path ["
                                    + bucketPath
                                    + "] indicates bucket_key ["
                                    + parsedPath.get(currElement).key()
                                    + "] at position ["
                                    + currElement
                                    + "] but encountered on agg ["
                                    + currentAgg.getName()
                                    + "] which is not a multi_bucket aggregation"
                            );
                        }
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
                List<String> sublistedPath = AggregationPath.pathElementsAsStringList(parsedPath.subList(currElement, parsedPath.size()));
                // First element is the current agg, so we want the rest of the path
                sublistedPath = sublistedPath.subList(1, sublistedPath.size());
                InternalMultiBucketAggregation<?, ?> multiBucketsAgg = (InternalMultiBucketAggregation<?, ?>) currentAgg;
                List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = multiBucketsAgg.getBuckets();
                List<Double> values = new ArrayList<>(buckets.size());
                List<Long> docCounts = new ArrayList<>(buckets.size());
                List<Integer> bucketIndexes = new ArrayList<>(buckets.size());
                int bucketCount = 0;
                int totalBuckets = buckets.size();
                for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
                    Double bucketValue = BucketHelpers.resolveBucketValue(multiBucketsAgg, bucket, sublistedPath, gapPolicy);
                    if (excludeLastBucket && bucketCount >= totalBuckets - 1) {
                        continue;
                    }
                    if (bucketValue == null || Double.isNaN(bucketValue)) {
                        if (gapPolicy.isSkippable) {
                            bucketCount++;
                            continue;
                        }
                        throw new AggregationExecutionException(
                            "missing or invalid bucket value found for path ["
                                + bucketPath
                                + "] in bucket ["
                                + bucket.getKeyAsString()
                                + "]"
                        );
                    }
                    bucketIndexes.add(bucketCount++);
                    values.add(bucketValue);
                    docCounts.add(bucket.getDocCount());
                }
                return Optional.of(
                    new DoubleBucketValues(
                        docCounts.stream().mapToLong(Long::longValue).toArray(),
                        values.stream().mapToDouble(Double::doubleValue).toArray(),
                        bucketCount == bucketIndexes.size() ? new int[0] : bucketIndexes.stream().mapToInt(Integer::intValue).toArray()
                    )
                );
            }
        }
        return Optional.empty();
    }

    public static Optional<InternalMultiBucketAggregation.InternalBucket> extractBucket(
        String bucketPath,
        Aggregations aggregations,
        int bucket
    ) {
        List<String> parsedPath = AggregationPath.parse(bucketPath).getPathElementsAsStringList();
        for (Aggregation aggregation : aggregations) {
            if (aggregation.getName().equals(parsedPath.get(0))) {
                InternalMultiBucketAggregation<?, ?> multiBucketsAgg = (InternalMultiBucketAggregation<?, ?>) aggregation;
                List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = multiBucketsAgg.getBuckets();
                if (bucket < buckets.size() && bucket >= 0) {
                    return Optional.of(buckets.get(bucket));
                }
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
        private final int[] buckets;

        public DoubleBucketValues(long[] docCounts, double[] values) {
            this(docCounts, values, new int[0]);
        }

        public DoubleBucketValues(long[] docCounts, double[] values, int[] buckets) {
            this.docCounts = docCounts;
            this.values = values;
            this.buckets = buckets;
        }

        public long[] getDocCounts() {
            return docCounts;
        }

        public double[] getValues() {
            return values;
        }

        public int getBucketIndex(int bucketPos) {
            if (buckets.length == 0) {
                return bucketPos;
            }
            return buckets[bucketPos];
        }
    }

}
