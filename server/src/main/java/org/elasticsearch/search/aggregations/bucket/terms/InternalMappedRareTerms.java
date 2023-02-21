/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.SetBackedScalingCuckooFilter;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class InternalMappedRareTerms<A extends InternalRareTerms<A, B>, B extends InternalRareTerms.Bucket<B>> extends
    InternalRareTerms<A, B> {

    protected DocValueFormat format;
    protected List<B> buckets;
    protected Map<String, B> bucketMap;

    final SetBackedScalingCuckooFilter filter;

    protected final Logger logger = LogManager.getLogger(getClass());

    InternalMappedRareTerms(
        String name,
        BucketOrder order,
        Map<String, Object> metadata,
        DocValueFormat format,
        List<B> buckets,
        long maxDocCount,
        SetBackedScalingCuckooFilter filter
    ) {
        super(name, order, maxDocCount, metadata);
        this.format = format;
        this.buckets = buckets;
        this.filter = filter;
    }

    public long getMaxDocCount() {
        return maxDocCount;
    }

    SetBackedScalingCuckooFilter getFilter() {
        return filter;
    }

    /**
     * Read from a stream.
     */
    InternalMappedRareTerms(StreamInput in, Bucket.Reader<B> bucketReader) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        buckets = in.readList(stream -> bucketReader.read(stream, format));
        filter = new SetBackedScalingCuckooFilter(in, Randomness.get());
    }

    @Override
    protected void writeTermTypeInfoTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeList(buckets);
        filter.writeTo(out);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        Map<Object, List<B>> buckets = new HashMap<>();
        InternalRareTerms<A, B> referenceTerms = null;
        SetBackedScalingCuckooFilter filter = null;

        for (InternalAggregation aggregation : aggregations) {
            // Unmapped rare terms don't have a cuckoo filter so we'll skip all this work
            // and save some type casting headaches later.
            if (aggregation.canLeadReduction() == false) {
                continue;
            }

            @SuppressWarnings("unchecked")
            InternalRareTerms<A, B> terms = (InternalRareTerms<A, B>) aggregation;
            if (referenceTerms == null && aggregation.getClass().equals(UnmappedRareTerms.class) == false) {
                referenceTerms = terms;
            }
            if (referenceTerms != null
                && referenceTerms.getClass().equals(terms.getClass()) == false
                && terms.getClass().equals(UnmappedRareTerms.class) == false) {
                // control gets into this loop when the same field name against which the query is executed
                // is of different types in different indices.
                throw new AggregationExecutionException(
                    "Merging/Reducing the aggregations failed when computing the aggregation ["
                        + referenceTerms.getName()
                        + "] because the field you gave in the aggregation query existed as two different "
                        + "types in two different indices"
                );
            }
            for (B bucket : terms.getBuckets()) {
                List<B> bucketList = buckets.computeIfAbsent(bucket.getKey(), k -> new ArrayList<>());
                bucketList.add(bucket);
            }

            SetBackedScalingCuckooFilter otherFilter = ((InternalMappedRareTerms) aggregation).getFilter();
            if (filter == null) {
                filter = new SetBackedScalingCuckooFilter(otherFilter.getThreshold(), otherFilter.getRng(), otherFilter.getFpp());
            }
            filter.merge(otherFilter);

        }

        final List<B> rare = new ArrayList<>();
        for (List<B> sameTermBuckets : buckets.values()) {
            final B b = reduceBucket(sameTermBuckets, reduceContext);
            if ((b.getDocCount() <= maxDocCount && containsTerm(filter, b) == false)) {
                rare.add(b);
                reduceContext.consumeBucketsAndMaybeBreak(1);
            } else if (b.getDocCount() > maxDocCount) {
                // this term has gone over threshold while merging, so add it to the filter.
                // Note this may happen during incremental reductions too
                addToFilter(filter, b);
            }
        }
        CollectionUtil.introSort(rare, order.comparator());
        return createWithFilter(name, rare, filter);
    }

    @Override
    public A finalizeSampling(SamplingContext samplingContext) {
        return createWithFilter(
            name,
            getBuckets().stream()
                .map(
                    b -> createBucket(
                        samplingContext.scaleUp(b.getDocCount()),
                        InternalAggregations.finalizeSampling(b.aggregations, samplingContext),
                        b
                    )
                )
                .toList(),
            filter
        );
    }

    public abstract boolean containsTerm(SetBackedScalingCuckooFilter filter, B bucket);

    public abstract void addToFilter(SetBackedScalingCuckooFilter filter, B bucket);

    @Override
    public List<B> getBuckets() {
        return buckets;
    }

    @Override
    public B getBucketByKey(String term) {
        if (bucketMap == null) {
            bucketMap = buckets.stream().collect(Collectors.toMap(InternalRareTerms.Bucket::getKeyAsString, Function.identity()));
        }
        return bucketMap.get(term);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        InternalMappedRareTerms<?, ?> that = (InternalMappedRareTerms<?, ?>) obj;
        return Objects.equals(buckets, that.buckets) && Objects.equals(format, that.format) && Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), buckets, format, filter);
    }

    @Override
    public final XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return doXContentCommon(builder, params, buckets);
    }
}
