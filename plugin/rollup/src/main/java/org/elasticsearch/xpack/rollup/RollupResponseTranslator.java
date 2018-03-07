/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation.InternalBucket;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.xpack.core.rollup.RollupField;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * This class contains static utilities that combine the responses from an msearch
 * with rollup + non-rollup agg trees into a single, regular search response
 * representing the union
 *
 */
public class RollupResponseTranslator {

    private static final Logger logger = Loggers.getLogger(RollupResponseTranslator.class);

    /**
     * Combines an msearch with rollup + non-rollup aggregations into a SearchResponse
     * representing the union of the two responses.  The response format is identical to
     * a non-rollup search response (aka a "normal aggregation" response).
     *
     * If the MSearch Response returns the following:
     *
     * <pre>{@code
     * [
     *   {
     *     "took":228,
     *     "timed_out":false,
     *     "_shards":{...},
     *     "hits":{...},
     *     "aggregations":{
     *        "histo":{
     *           "buckets":[
     *              {
     *                 "key_as_string":"2017-05-15T00:00:00.000Z",
     *                 "key":1494806400000,
     *                 "doc_count":1,
     *                 "the_max":{
     *                    "value":1.0
     *                 }
     *              }
     *           ]
     *        }
     *     }
     *   },
     *   {
     *     "took":205,
     *     "timed_out":false,
     *     "_shards":{...},
     *     "hits":{...},
     *     "aggregations":{
     *        "filter_histo":{
     *           "doc_count":1,
     *           "histo":{
     *              "buckets":[
     *                 {
     *                    "key_as_string":"2017-05-14T00:00:00.000Z",
     *                    "key":1494720000000,
     *                    "doc_count":1,
     *                    "the_max":{
     *                       "value":19995.0
     *                    },
     *                    "histo._count":{
     *                       "value":1.0E9
     *                    }
     *                 }
     *              ]
     *           }
     *        }
     *     }
     *   }
     * }</pre>
     *
     * It would be collapsed into:
     *
     * <pre>{@code
     * {
     *   "took": 228,
     *   "timed_out": false,
     *   "_shards": {...},
     *   "hits": {...},
     *   "aggregations": {
     *     "histo": {
     *       "buckets": [
     *         {
     *           "key_as_string": "2017-05-14T00:00:00.000Z",
     *           "key": 1494720000000,
     *           "doc_count": 1000000000,
     *           "the_max": {
     *             "value": 19995
     *           }
     *         },
     *         {
     *           "key_as_string": "2017-05-15T00:00:00.000Z",
     *           "key": 1494806400000,
     *           "doc_count": 1,
     *           "the_max": {
     *             "value": 1
     *           }
     *         }
     *       ]
     *     }
     *   }
     * }
     * }</pre>
     *
     * It essentially takes the conventions listed in {@link RollupRequestTranslator} and processes them
     * so that the final product looks like a regular aggregation response, allowing it to be
     * reduced/merged into the response from the un-rolled index
     *
     * @param normalResponse The MultiSearch response from a non-rollup msearch
     */
    public static SearchResponse verifyResponse(MultiSearchResponse.Item normalResponse) {
        if (normalResponse.isFailure()) {
            throw new RuntimeException(normalResponse.getFailureMessage(), normalResponse.getFailure());
        }
        return normalResponse.getResponse();
    }

    public static SearchResponse translateResponse(MultiSearchResponse.Item rolledResponse,
                                                 InternalAggregation.ReduceContext reduceContext) {
        if (rolledResponse.isFailure()) {
           throw new RuntimeException(rolledResponse.getFailureMessage(), rolledResponse.getFailure());
        }
        return doCombineResponse(null, rolledResponse.getResponse(), reduceContext);
    }

    public static SearchResponse combineResponses(MultiSearchResponse.Item normalResponse, MultiSearchResponse.Item rolledResponse,
                                                 InternalAggregation.ReduceContext reduceContext) {
        boolean normalMissing = false;
        if (normalResponse.isFailure()) {
            Exception e = normalResponse.getFailure();
            // If we have a rollup response we can tolerate a missing normal response
            if (e instanceof IndexNotFoundException) {
                logger.warn("\"Live\" index not found during rollup search.", e);
                normalMissing = true;
            } else {
                throw new RuntimeException(normalResponse.getFailureMessage(), normalResponse.getFailure());
            }
        }
        if (rolledResponse.isFailure()) {
            Exception e = rolledResponse.getFailure();
            // If we have a normal response we can tolerate a missing rollup response, although it theoretically
            // should be handled by a different code path (verifyResponse)
            if (e instanceof IndexNotFoundException && normalMissing == false) {
                logger.warn("\"Rollup\" index not found during rollup search.", e);
                return verifyResponse(normalResponse);
            } else {
                throw new RuntimeException(rolledResponse.getFailureMessage(), rolledResponse.getFailure());
            }
        }
        return doCombineResponse(normalResponse.getResponse(), rolledResponse.getResponse(), reduceContext);
    }

    private static SearchResponse doCombineResponse(SearchResponse originalResponse, SearchResponse rolledResponse,
                                                  InternalAggregation.ReduceContext reduceContext) {

        final InternalAggregations originalAggs = originalResponse != null
                ? (InternalAggregations)originalResponse.getAggregations()
                : InternalAggregations.EMPTY;

        if (rolledResponse.getAggregations() == null || rolledResponse.getAggregations().asList().size() == 0) {
            logger.debug(Strings.toString(rolledResponse));
            throw new RuntimeException("Expected to find aggregations in rollup response, but none found.");
        }

        List<InternalAggregation> unrolledAggs = rolledResponse.getAggregations().asList().stream()
                .map(agg -> {
                    // We expect a filter agg here because the rollup convention is that all translated aggs
                    // will start with a filter, containing various agg-specific predicates.  If there
                    // *isn't* a filter agg here, something has gone very wrong!
                    if ((agg instanceof InternalFilter) == false) {
                        throw new RuntimeException("Expected [" +agg.getName()
                                + "] to be a FilterAggregation, but was ["
                                + agg.getClass().getSimpleName() + "]");
                    }

                    return unrollAgg(((InternalFilter)agg).getAggregations(), originalAggs);
                })
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        // The combination process returns a tree that is identical to the non-rolled
        // which means we can use aggregation's reduce method to combine, just as if
        // it was a result from another shard
        List<InternalAggregations> toReduce = new ArrayList<>(2);
        toReduce.add(new InternalAggregations(unrolledAggs));
        if (originalAggs.asList().size() != 0) {
            toReduce.add(originalAggs);
        }

        InternalAggregations finalCombinedAggs = InternalAggregations.reduce(toReduce,
                new InternalAggregation.ReduceContext(reduceContext.bigArrays(), reduceContext.scriptService(), true));

        // TODO allow profiling in the future
        InternalSearchResponse combinedInternal = new InternalSearchResponse(SearchHits.empty(), finalCombinedAggs,
                null, null, rolledResponse.isTimedOut(),
                rolledResponse.isTerminatedEarly(), rolledResponse.getNumReducePhases());

        int totalShards = rolledResponse.getTotalShards();
        int sucessfulShards = rolledResponse.getSuccessfulShards();
        int skippedShards = rolledResponse.getSkippedShards();
        long took = rolledResponse.getTook().getMillis();

        if (originalResponse != null) {
            totalShards += originalResponse.getTotalShards();
            sucessfulShards += originalResponse.getSuccessfulShards();
            skippedShards += originalResponse.getSkippedShards();
            took = Math.max(took, originalResponse.getTook().getMillis());
        }

        // Shard failures are ignored atm, so returning an empty array is fine
        return new SearchResponse(combinedInternal, null, totalShards, sucessfulShards, skippedShards,
                took, ShardSearchFailure.EMPTY_ARRAY, rolledResponse.getClusters());
    }

    /**
     * Takes an aggregation with rollup conventions and unrolls into a "normal" agg tree
     *

     * @param rolled   The rollup aggregation that we wish to unroll
     * @param original The unrolled, "live" aggregation (if it exists) that matches the current rolled aggregation
     *
     * @return An unrolled aggregation that mimics the structure of `base`, allowing reduction
     */
    private static List<InternalAggregation> unrollAgg(InternalAggregations rolled, InternalAggregations original) {
        return rolled.asList().stream()
                .filter(subAgg -> !subAgg.getName().endsWith("." + RollupField.COUNT_FIELD))
                .map(agg -> {
                    // During the translation process, some aggregations' doc_counts are stored in accessory
                    // `sum` metric aggs, so we may need to extract that.  Unfortunately, structure of multibucket vs
                    // leaf metric is slightly different; multibucket count is stored per-bucket in a sub-agg, while
                    // metric is "next" to the metric as a sibling agg.
                    //
                    // So we only look for a count if this is not a multibucket, as multibuckets will handle
                    // the doc_count themselves on a per-bucket basis.
                    //
                    long count = -1;
                    if (agg instanceof InternalMultiBucketAggregation == false) {
                        count = getAggCount(agg, rolled.getAsMap());
                    }

                    return unrollAgg((InternalAggregation)agg, original.get(agg.getName()), count);
                }).collect(Collectors.toList());
    }

    /**
     * Takes an aggregation with rollup conventions and unrolls into a "normal" agg tree
     *
     * @param rolled      The rollup aggregation that we wish to unroll
     * @param originalAgg The unrolled, "live" aggregation (if it exists) that matches the current rolled aggregation
     * @param count       The doc_count for `rolled`, required by some aggs (e.g. avg)
     *
     * @return An unrolled aggregation that mimics the structure of base, allowing reduction
     */
    protected static InternalAggregation unrollAgg(InternalAggregation rolled, InternalAggregation originalAgg, long count) {

        if (rolled instanceof InternalMultiBucketAggregation) {
            return unrollMultiBucket((InternalMultiBucketAggregation) rolled, (InternalMultiBucketAggregation) originalAgg);
        } else if (rolled instanceof SingleValue) {
            return unrollMetric((SingleValue) rolled, count);
        } else {
            throw new RuntimeException("Unable to unroll aggregation tree.  Aggregation ["
                    + rolled.getName() + "] is of type [" + rolled.getClass().getSimpleName() + "] which is " +
                    "currently unsupported.");
        }
    }

    /**
     * Unrolls Multibucket aggregations (e.g. terms, histograms, etc).  This overload signature should be
     * called by other internal methods in this class, rather than directly calling the per-type methods.
     */
    @SuppressWarnings("unchecked")
    private static InternalAggregation unrollMultiBucket(InternalMultiBucketAggregation rolled, InternalMultiBucketAggregation original) {

        // The only thing unique between all the multibucket agg is the type of bucket they
        // need, so this if/else simply creates specialized closures that return the appropriate
        // bucket type.  Otherwise the heavy-lifting is in
        // {@link #unrollMultiBucket(InternalMultiBucketAggregation, InternalMultiBucketAggregation, TriFunction)}
        if (rolled instanceof InternalDateHistogram) {
            return unrollMultiBucket(rolled, original, (bucket, bucketCount, subAggs) -> {
                long key = ((InternalDateHistogram) rolled).getKey(bucket).longValue();
                DocValueFormat formatter = ((InternalDateHistogram.Bucket)bucket).getFormatter();
                return new InternalDateHistogram.Bucket(key, bucketCount,
                        ((InternalDateHistogram.Bucket) bucket).getKeyed(), formatter, subAggs);
            });
        } else if (rolled instanceof InternalHistogram) {
            return unrollMultiBucket(rolled, original, (bucket, bucketCount, subAggs) -> {
                long key = ((InternalHistogram) rolled).getKey(bucket).longValue();
                DocValueFormat formatter = ((InternalHistogram.Bucket)bucket).getFormatter();
                return new InternalHistogram.Bucket(key, bucketCount, ((InternalHistogram.Bucket) bucket).getKeyed(), formatter, subAggs);
            });
        } else if (rolled instanceof StringTerms) {
            return unrollMultiBucket(rolled, original, (bucket, bucketCount, subAggs) -> {
                BytesRef key = new BytesRef(bucket.getKeyAsString().getBytes(StandardCharsets.UTF_8));
                //TODO expose getFormatter(), keyed upstream in Core
                return new StringTerms.Bucket(key, bucketCount, subAggs, false, 0, DocValueFormat.RAW);
            });
        } else if (rolled instanceof LongTerms) {
            return unrollMultiBucket(rolled, original, (bucket, bucketCount, subAggs) -> {
                long key = (long)bucket.getKey();
                //TODO expose getFormatter(), keyed upstream in Core
                return new LongTerms.Bucket(key, bucketCount, subAggs, false, 0, DocValueFormat.RAW);
            });
        } else {
            throw new RuntimeException("Unable to unroll aggregation tree.  Aggregation ["
                    + rolled.getName() + "] is of type [" + rolled.getClass().getSimpleName() + "] which is " +
                    "currently unsupported.");
        }

    }

    /**
     * Helper method which unrolls a generic multibucket agg. Prefer to use the other overload
     * as a consumer of the API
     *
     * @param source The rolled aggregation that we wish to unroll
     * @param bucketFactory A Trifunction which generates new buckets for the given type of multibucket
     */
    private static <A  extends InternalMultiBucketAggregation,
                    B extends InternalBucket,
                    T extends InternalMultiBucketAggregation<A, B>>
    InternalAggregation unrollMultiBucket(T source, T original,
                                          TriFunction<InternalBucket, Long, InternalAggregations, B> bucketFactory) {

        Set<Object> keys = new HashSet<>();

        if (original != null) {
            original.getBuckets().forEach(b -> keys.add(b.getKey()));
        }

        // Iterate over the buckets in the multibucket
        List<B> buckets = source.getBuckets()
                .stream()
                .filter(b -> keys.contains(b.getKey()) == false) // If the original has this key, ignore the rolled version
                .map(bucket -> {
                    // Grab the value from the count agg (if it exists), which represents this bucket's doc_count
                    long bucketCount = getAggCount(source, bucket.getAggregations().getAsMap());

                    // Then iterate over the subAggs in the bucket
                    InternalAggregations subAggs = unrollSubAggsFromMulti(bucket);

                    return bucketFactory.apply(bucket, bucketCount, subAggs);
                }).collect(Collectors.toList());
        return source.create(buckets);
    }

    /**
     * Generic method to help iterate over sub-aggregation buckets and recursively unroll
     *
     * @param bucket The current bucket that we wish to unroll
     */
    private static InternalAggregations unrollSubAggsFromMulti(InternalBucket bucket) {
        // Iterate over the subAggs in each bucket
        return new InternalAggregations(bucket.getAggregations()
                .asList().stream()
                // Avoid any rollup count metrics, as that's not a true "sub-agg" but rather agg
                // added by the rollup for accounting purposes (e.g. doc_count)
                .filter(subAgg -> !subAgg.getName().endsWith("." + RollupField.COUNT_FIELD))
                .map(subAgg -> {

                    long count = getAggCount(subAgg, bucket.getAggregations().asMap());

                    return unrollAgg((InternalAggregation) subAgg, null, count);
                }).collect(Collectors.toList()));
    }

    private static InternalAggregation unrollMetric(SingleValue metric, long count) {
        // TODO comment from Colin in review:
        // RAW won't work here long term since if you do a max on e.g. a date field it will
        // render differently for the rolled up and non-rolled up results. At the moment
        // the formatter is not exposed on the internal agg objects but I think this is
        // something we can discuss exposing
        if (metric instanceof InternalMax || metric instanceof InternalMin) {
            return metric;
        } else if (metric instanceof InternalSum) {
            // If count is anything other than -1, this sum is actually an avg
            if (count != -1) {
                return new InternalAvg(metric.getName(), metric.value(), count, DocValueFormat.RAW,
                        metric.pipelineAggregators(), metric.getMetaData());
            }
            return metric;
        } else {
            throw new RuntimeException("Unable to unroll metric.  Aggregation ["
                    + metric.getName() + "] is of type [" + metric.getClass().getSimpleName() + "] which is " +
                    "currently unsupported.");
        }
    }

    private static long getAggCount(Aggregation agg, Map<String, Aggregation> aggMap) {
        String countPath = null;

        if (agg.getType().equals(DateHistogramAggregationBuilder.NAME)
                || agg.getType().equals(HistogramAggregationBuilder.NAME)
                || agg.getType().equals(StringTerms.NAME) || agg.getType().equals(LongTerms.NAME)
                || agg.getType().equals(SumAggregationBuilder.NAME)) {
            countPath = RollupField.formatCountAggName(agg.getName());
        }

        if (countPath != null && aggMap.get(countPath) != null) {
            // we always set the count fields to Sum aggs, so this is safe
            assert (aggMap.get(countPath) instanceof InternalSum);
            return (long)((InternalSum) aggMap.get(countPath)).getValue();
        }

        return -1;
    }
}
