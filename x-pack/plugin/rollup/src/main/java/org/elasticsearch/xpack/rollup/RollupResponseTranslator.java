/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.TriFunction;
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
import org.elasticsearch.search.aggregations.metrics.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.InternalMax;
import org.elasticsearch.search.aggregations.metrics.InternalMin;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.aggregations.metrics.InternalSum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.xpack.core.rollup.RollupField;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;


/**
 * This class contains static utilities that combine the responses from an msearch
 * with rollup + non-rollup agg trees into a single, regular search response
 * representing the union
 *
 */
public class RollupResponseTranslator {

    /**
     * Verifies a live-only search response.  Essentially just checks for failure then returns
     * the response since we have no work to do
     */
    public static SearchResponse verifyResponse(MultiSearchResponse.Item normalResponse) throws Exception {
        if (normalResponse.isFailure()) {
            throw normalResponse.getFailure();
        }
        return normalResponse.getResponse();
    }

    /**
     * Translates a rollup-only search response back into the expected convention.  Similar to
     * {@link #combineResponses(MultiSearchResponse.Item[], InternalAggregation.ReduceContext)} except it only
     * has to deal with the rollup response (no live response)
     *
     * See {@link #combineResponses(MultiSearchResponse.Item[], InternalAggregation.ReduceContext)} for more details
     * on the translation conventions
     */
    public static SearchResponse translateResponse(MultiSearchResponse.Item[] rolledMsearch,
                                                 InternalAggregation.ReduceContext reduceContext) throws Exception {

        assert rolledMsearch.length > 0;
        List<SearchResponse> responses = new ArrayList<>();
        for (MultiSearchResponse.Item item : rolledMsearch) {
            if (item.isFailure()) {
                Exception e = item.getFailure();

                // If an index was deleted after execution, give a hint to the user that this is a transient error
                if (e instanceof IndexNotFoundException) {
                    throw new ResourceNotFoundException("Index [" + ((IndexNotFoundException) e).getIndex().getName()
                        + "] was not found, likely because it was deleted while the request was in-flight. " +
                        "Rollup does not support partial search results, please try the request again.");
                }

                // Otherwise just throw
                throw e;
            }

            // No error, add to responses
            responses.add(item.getResponse());
        }

        assert responses.size() > 0;
        return doCombineResponse(null, responses, reduceContext);
    }

    /**
     * Combines an msearch with rollup + live aggregations into a SearchResponse
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
     * @param msearchResponses The responses from the msearch, where the first response is the live-index response
     */
    public static SearchResponse combineResponses(MultiSearchResponse.Item[] msearchResponses,
                                                  InternalAggregation.ReduceContext reduceContext) throws Exception {

        assert msearchResponses.length >= 2;

        boolean first = true;
        SearchResponse liveResponse = null;
        List<SearchResponse> rolledResponses = new ArrayList<>();
        for (MultiSearchResponse.Item item : msearchResponses) {
            if (item.isFailure()) {
                Exception e = item.getFailure();

                // If an index was deleted after execution, give a hint to the user that this is a transient error
                if (e instanceof IndexNotFoundException) {
                    throw new ResourceNotFoundException("Index [" + ((IndexNotFoundException) e).getIndex() + "] was not found, " +
                        "likely because it was deleted while the request was in-flight. Rollup does not support partial search results, " +
                        "please try the request again.", e);
                }

                // Otherwise just throw
                throw e;
            }

            // No error, add to responses
            if (first) {
                liveResponse = item.getResponse();
            } else {
                rolledResponses.add(item.getResponse());
            }
            first = false;
        }

        // If we only have a live index left, just return it directly.  We know it can't be an error already
        if (rolledResponses.isEmpty() && liveResponse != null) {
            return liveResponse;
        } else if (rolledResponses.isEmpty()) {
            throw new ResourceNotFoundException("No indices (live or rollup) found during rollup search");
        }

        return doCombineResponse(liveResponse, rolledResponses, reduceContext);
    }

    private static SearchResponse doCombineResponse(SearchResponse liveResponse, List<SearchResponse> rolledResponses,
                                                  InternalAggregation.ReduceContext reduceContext) {

        final InternalAggregations liveAggs = liveResponse != null
                ? (InternalAggregations)liveResponse.getAggregations()
                : InternalAggregations.EMPTY;

        int missingRollupAggs = rolledResponses.stream().mapToInt(searchResponse -> {
            if (searchResponse == null
                || searchResponse.getAggregations() == null
                || searchResponse.getAggregations().asList().size() == 0) {
                return 1;
            }
            return 0;
        }).sum();

        // We had no rollup aggs, so there is nothing to process
        if (missingRollupAggs == rolledResponses.size()) {
            // Return an empty response, but make sure we include all the shard, failure, etc stats
            return mergeFinalResponse(liveResponse, rolledResponses, InternalAggregations.EMPTY);
        } else if (missingRollupAggs > 0 && missingRollupAggs != rolledResponses.size()) {
            // We were missing some but not all the aggs, unclear how to handle this.  Bail.
            throw new RuntimeException("Expected to find aggregations in rollup response, but none found.");
        }

        // The combination process returns a tree that is identical to the non-rolled
        // which means we can use aggregation's reduce method to combine, just as if
        // it was a result from another shard
        InternalAggregations currentTree = InternalAggregations.EMPTY;
        InternalAggregation.ReduceContext finalReduceContext = InternalAggregation.ReduceContext.forFinalReduction(
                reduceContext.bigArrays(), reduceContext.scriptService(), b -> {}, PipelineTree.EMPTY);
        for (SearchResponse rolledResponse : rolledResponses) {
            List<InternalAggregation> unrolledAggs = new ArrayList<>(rolledResponse.getAggregations().asList().size());
            for (Aggregation agg : rolledResponse.getAggregations()) {
                // We expect a filter agg here because the rollup convention is that all translated aggs
                // will start with a filter, containing various agg-specific predicates.  If there
                // *isn't* a filter agg here, something has gone very wrong!
                if ((agg instanceof InternalFilter) == false) {
                    throw new RuntimeException("Expected [" +agg.getName()
                            + "] to be a FilterAggregation, but was ["
                            + agg.getClass().getSimpleName() + "]");
                }
                unrolledAggs.addAll(unrollAgg(((InternalFilter)agg).getAggregations(), liveAggs, currentTree));
            }

            // Iteratively merge in each new set of unrolled aggs, so that we can identify/fix overlapping doc_counts
            // in the next round of unrolling
            InternalAggregations finalUnrolledAggs = InternalAggregations.from(unrolledAggs);
            currentTree = InternalAggregations.reduce(Arrays.asList(currentTree, finalUnrolledAggs), finalReduceContext);
        }

        // Add in the live aggregations if they exist
        if (liveAggs.asList().size() != 0) {
            // TODO it looks like this passes the "final" reduce context more than once.
            // Once here and once in the for above. That is bound to cause trouble.
            currentTree = InternalAggregations.reduce(Arrays.asList(currentTree, liveAggs), finalReduceContext);
        }

        return mergeFinalResponse(liveResponse, rolledResponses, currentTree);
    }

    private static SearchResponse mergeFinalResponse(SearchResponse liveResponse, List<SearchResponse> rolledResponses,
                                              InternalAggregations aggs) {

        int totalShards = rolledResponses.stream().mapToInt(SearchResponse::getTotalShards).sum();
        int sucessfulShards = rolledResponses.stream().mapToInt(SearchResponse::getSuccessfulShards).sum();
        int skippedShards = rolledResponses.stream().mapToInt(SearchResponse::getSkippedShards).sum();
        long took = rolledResponses.stream().mapToLong(r -> r.getTook().getMillis()).sum() ;

        boolean isTimedOut = rolledResponses.stream().anyMatch(SearchResponse::isTimedOut);
        boolean isTerminatedEarly = rolledResponses.stream()
            .filter(r -> r.isTerminatedEarly() != null)
            .anyMatch(SearchResponse::isTerminatedEarly);
        int numReducePhases = rolledResponses.stream().mapToInt(SearchResponse::getNumReducePhases).sum();

        if (liveResponse != null) {
            totalShards += liveResponse.getTotalShards();
            sucessfulShards += liveResponse.getSuccessfulShards();
            skippedShards += liveResponse.getSkippedShards();
            took = Math.max(took, liveResponse.getTook().getMillis());
            isTimedOut = isTimedOut && liveResponse.isTimedOut();
            isTerminatedEarly = isTerminatedEarly && liveResponse.isTerminatedEarly();
            numReducePhases += liveResponse.getNumReducePhases();
        }

        InternalSearchResponse combinedInternal = new InternalSearchResponse(SearchHits.empty(), aggs, null, null,
            isTimedOut, isTerminatedEarly, numReducePhases);

        // Shard failures are ignored atm, so returning an empty array is fine
        return new SearchResponse(combinedInternal, null, totalShards, sucessfulShards, skippedShards,
            took, ShardSearchFailure.EMPTY_ARRAY, rolledResponses.get(0).getClusters());
    }

    /**
     * Takes an aggregation with rollup conventions and unrolls into a "normal" agg tree
     *

     * @param rolled   The rollup aggregation that we wish to unroll
     * @param original The unrolled, "live" aggregation (if it exists) that matches the current rolled aggregation
     *
     * @return An unrolled aggregation that mimics the structure of `base`, allowing reduction
     */
    private static List<InternalAggregation> unrollAgg(InternalAggregations rolled, InternalAggregations original,
                                                       InternalAggregations currentTree) {
        return rolled.asList().stream()
                .filter(subAgg -> subAgg.getName().endsWith("." + RollupField.COUNT_FIELD) == false)
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

                    return unrollAgg((InternalAggregation)agg, original.get(agg.getName()), currentTree.get(agg.getName()), count);
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
    protected static InternalAggregation unrollAgg(InternalAggregation rolled, InternalAggregation originalAgg,
                                                   InternalAggregation currentTree, long count) {

        if (rolled instanceof InternalMultiBucketAggregation) {
            return unrollMultiBucket((InternalMultiBucketAggregation) rolled, (InternalMultiBucketAggregation) originalAgg,
                    (InternalMultiBucketAggregation) currentTree);
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
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static InternalAggregation unrollMultiBucket(InternalMultiBucketAggregation rolled, InternalMultiBucketAggregation original,
                                                         InternalMultiBucketAggregation currentTree) {

        // The only thing unique between all the multibucket agg is the type of bucket they
        // need, so this if/else simply creates specialized closures that return the appropriate
        // bucket type.  Otherwise the heavy-lifting is in
        // {@link #unrollMultiBucket(InternalMultiBucketAggregation, InternalMultiBucketAggregation, TriFunction)}
        if (rolled instanceof InternalDateHistogram) {
            return unrollMultiBucket(rolled, original, currentTree, (bucket, bucketCount, subAggs) -> {
                long key = ((InternalDateHistogram) rolled).getKey(bucket).longValue();
                DocValueFormat formatter = ((InternalDateHistogram.Bucket)bucket).getFormatter();
                assert bucketCount >= 0;
                return new InternalDateHistogram.Bucket(key, bucketCount,
                        ((InternalDateHistogram.Bucket) bucket).getKeyed(), formatter, subAggs);
            });
        } else if (rolled instanceof InternalHistogram) {
            return unrollMultiBucket(rolled, original, currentTree, (bucket, bucketCount, subAggs) -> {
                long key = ((InternalHistogram) rolled).getKey(bucket).longValue();
                DocValueFormat formatter = ((InternalHistogram.Bucket)bucket).getFormatter();
                assert bucketCount >= 0;
                return new InternalHistogram.Bucket(key, bucketCount, ((InternalHistogram.Bucket) bucket).getKeyed(), formatter, subAggs);
            });
        } else if (rolled instanceof StringTerms) {
            return unrollMultiBucket(rolled, original, currentTree, (bucket, bucketCount, subAggs) -> {

                BytesRef key = new BytesRef(bucket.getKeyAsString().getBytes(StandardCharsets.UTF_8));
                assert bucketCount >= 0;
                //TODO expose getFormatter(), keyed upstream in Core
                return new StringTerms.Bucket(key, bucketCount, subAggs, false, 0, DocValueFormat.RAW);
            });
        } else if (rolled instanceof LongTerms) {
            return unrollMultiBucket(rolled, original, currentTree, (bucket, bucketCount, subAggs) -> {
                long key = (long)bucket.getKey();
                assert bucketCount >= 0;
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
    private static <A  extends InternalMultiBucketAggregation<A, B>,
                    B extends InternalBucket,
                    T extends InternalMultiBucketAggregation<A, B>>
    InternalAggregation unrollMultiBucket(T source, T original, T currentTree,
                                          TriFunction<InternalBucket, Long, InternalAggregations, B> bucketFactory) {

        Map<Object, InternalBucket> originalKeys = new HashMap<>();
        Map<Object, InternalBucket> currentKeys = new HashMap<>();

        if (original != null) {
            original.getBuckets().forEach(b -> originalKeys.put(b.getKey(), b));
        }

        if (currentTree != null) {
            currentTree.getBuckets().forEach(b -> currentKeys.put(b.getKey(), b));
        }

        // Iterate over the buckets in the multibucket
        List<B> buckets = source.getBuckets()
                .stream()
                .filter(b -> originalKeys.containsKey(b.getKey()) == false) // If the original has this key, ignore the rolled version
                .map(bucket -> {

                    // Grab the value from the count agg (if it exists), which represents this bucket's doc_count
                    long bucketCount = getAggCount(source, bucket.getAggregations().getAsMap());

                    // Don't generate buckets if the doc count is zero
                    if (bucketCount == 0) {
                        return null;
                    }

                    // current, partially merged tree contains this key.  Defer to the existing doc_count if it is non-zero
                    if (currentKeys.containsKey(bucket.getKey()) && currentKeys.get(bucket.getKey()).getDocCount() != 0) {
                        // Unlike above where we return null if doc_count is zero, we return a doc_count: 0 bucket
                        // here because it may have sub-aggs that need merging, whereas above the bucket was just empty/null
                        bucketCount = 0;
                    }

                    // Then iterate over the subAggs in the bucket
                    InternalAggregations subAggs = unrollSubAggsFromMulti(bucket, originalKeys.get(bucket.getKey()),
                            currentKeys.get(bucket.getKey()));

                    return bucketFactory.apply(bucket, bucketCount, subAggs);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return source.create(buckets);
    }

    /**
     * Generic method to help iterate over sub-aggregation buckets and recursively unroll
     *
     * @param bucket The current bucket that we wish to unroll
     */
    private static InternalAggregations unrollSubAggsFromMulti(InternalBucket bucket, InternalBucket original, InternalBucket currentTree) {
        // Iterate over the subAggs in each bucket
        return InternalAggregations.from(bucket.getAggregations()
                .asList().stream()
                // Avoid any rollup count metrics, as that's not a true "sub-agg" but rather agg
                // added by the rollup for accounting purposes (e.g. doc_count)
                .filter(subAgg -> subAgg.getName().endsWith("." + RollupField.COUNT_FIELD) == false)
                .map(subAgg -> {

                    long count = getAggCount(subAgg, bucket.getAggregations().asMap());

                    InternalAggregation originalSubAgg = null;
                    if (original != null && original.getAggregations() != null) {
                        originalSubAgg = original.getAggregations().get(subAgg.getName());
                    }

                    InternalAggregation currentSubAgg = null;
                    if (currentTree != null && currentTree.getAggregations() != null) {
                        currentSubAgg = currentTree.getAggregations().get(subAgg.getName());
                    }

                    return unrollAgg((InternalAggregation) subAgg, originalSubAgg, currentSubAgg, count);
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
                // Note: Avgs have a slightly different name to prevent collision with empty bucket defaults
                return new InternalAvg(metric.getName().replace("." + RollupField.VALUE, ""), metric.value(), count, DocValueFormat.RAW,
                        metric.getMetadata());
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
                || agg.getType().equals(StringTerms.NAME) || agg.getType().equals(LongTerms.NAME)) {
            countPath = RollupField.formatCountAggName(agg.getName());
        } else if (agg.getType().equals(SumAggregationBuilder.NAME)) {
            // Note: Avgs have a slightly different name to prevent collision with empty bucket defaults
            countPath = RollupField.formatCountAggName(agg.getName().replace("." + RollupField.VALUE, ""));
        }

        if (countPath != null && aggMap.get(countPath) != null) {
            // we always set the count fields to Sum aggs, so this is safe
            assert aggMap.get(countPath) instanceof InternalSum;
            return (long)((InternalSum) aggMap.get(countPath)).getValue();
        }

        return -1;
    }
}
