/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CategorizeTextAggregator extends DeferableBucketAggregator {

    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final SourceLookup sourceLookup;
    private final BigArrays bigArrays;
    private final MappedFieldType fieldType;
    private final CategorizationAnalyzer analyzer;
    private final String sourceFieldName;
    private ObjectArray<CategorizationTokenTree> categorizers;
    private final int maxChildren;
    private final int maxDepth;
    private final double similarityThreshold;
    private final LongKeyedBucketOrds bucketOrds;

    protected CategorizeTextAggregator(
        String name,
        AggregatorFactories factories,
        AggregationContext context,
        Aggregator parent,
        String sourceFieldName,
        MappedFieldType fieldType,
        TermsAggregator.BucketCountThresholds bucketCountThresholds,
        int maxChildren,
        int maxDepth,
        double similarityThreshold,
        List<String> categorizationFilters,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, metadata);
        this.sourceLookup = context.lookup().source();
        this.sourceFieldName = sourceFieldName;
        this.fieldType = fieldType;
        CategorizationAnalyzerConfig categorizationAnalyzerConfig = CategorizationAnalyzerConfig.buildStandardCategorizationAnalyzer(
            categorizationFilters
        );
        this.analyzer = new CategorizationAnalyzer(context.getAnalysisRegistry(), categorizationAnalyzerConfig);
        this.bigArrays = context.bigArrays();
        this.categorizers = bigArrays().newObjectArray(1);
        this.maxChildren = maxChildren;
        this.maxDepth = maxDepth;
        this.similarityThreshold = similarityThreshold;
        this.bucketOrds = LongKeyedBucketOrds.build(bigArrays(), CardinalityUpperBound.MANY);
        this.bucketCountThresholds = bucketCountThresholds;
    }

    @Override
    protected void doClose() {
        super.doClose();
        this.analyzer.close();
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] ordsToCollect) throws IOException {
        InternalCategorizationAggregation.Bucket[][] topBucketsPerOrd =
            new InternalCategorizationAggregation.Bucket[ordsToCollect.length][];
        for (int ordIdx = 0; ordIdx < ordsToCollect.length; ordIdx++) {
            int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());
            PriorityQueue<InternalCategorizationAggregation.Bucket> ordered =
                new InternalCategorizationAggregation.BucketCountPriorityQueue(size);
            CategorizationTokenTree categorizationTokenTree = categorizers.get(ordsToCollect[ordIdx]);
            for (InternalCategorizationAggregation.Bucket bucket : categorizationTokenTree.toIntermediateBuckets()) {
                if (bucket.docCount < bucketCountThresholds.getShardMinDocCount()) {
                    continue;
                }
                ordered.insertWithOverflow(bucket);
            }
            topBucketsPerOrd[ordIdx] = new InternalCategorizationAggregation.Bucket[ordered.size()];
            for (int i = ordered.size() - 1; i >= 0; --i) {
                topBucketsPerOrd[ordIdx][i] = ordered.pop();
            }
        }
        buildSubAggsForAllBuckets(topBucketsPerOrd, b -> b.bucketOrd, (b, a) -> b.aggregations = a);
        InternalAggregation[] results = new InternalAggregation[ordsToCollect.length];
        for (int ordIdx = 0; ordIdx < ordsToCollect.length; ordIdx++) {
            InternalCategorizationAggregation.Bucket[] bucketArray = topBucketsPerOrd[ordIdx];
            Arrays.sort(bucketArray, Comparator.naturalOrder());
            results[ordIdx] = new InternalCategorizationAggregation(
                name,
                bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(),
                maxChildren,
                maxDepth,
                similarityThreshold,
                metadata(),
                Arrays.asList(bucketArray)
            );
        }
        return results;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalCategorizationAggregation(
            name,
            bucketCountThresholds.getRequiredSize(),
            bucketCountThresholds.getMinDocCount(),
            maxChildren,
            maxDepth,
            similarityThreshold,
            metadata()
        );
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        return new LeafBucketCollectorBase(sub, null) {

            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                collectFromSource(doc, owningBucketOrd);
            }

            private void collectFromSource(int doc, long owningBucketOrd) throws IOException {
                sourceLookup.setSegmentAndDocument(ctx, doc);
                Iterator<String> itr = sourceLookup.extractRawValues(sourceFieldName).stream().map(obj -> {
                    if (obj == null) {
                        return null;
                    }
                    if (obj instanceof BytesRef) {
                        return fieldType.valueForDisplay(obj).toString();
                    }
                    return obj.toString();
                }).iterator();
                while (itr.hasNext()) {
                    TokenStream ts = analyzer.tokenStream(fieldType.name(), itr.next());
                    processTokenStream(owningBucketOrd, ts, doc);
                }
            }

            private void processTokenStream(long owningBucketOrd, TokenStream ts, int doc) throws IOException {
                try {
                    CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
                    ts.reset();
                    ArrayList<BytesRef> tokens = new ArrayList<>();
                    while (ts.incrementToken()) {
                        tokens.add(new BytesRef(termAtt));
                    }
                    if (tokens.isEmpty()) {
                        return;
                    }
                    categorizers = bigArrays.grow(categorizers, owningBucketOrd + 1);
                    CategorizationTokenTree categorizer = categorizers.get(owningBucketOrd);
                    if (categorizer == null) {
                        categorizer = new CategorizationTokenTree(maxChildren, maxDepth, similarityThreshold);
                        addRequestCircuitBreakerBytes(categorizer.ramBytesUsed());
                        categorizers.set(owningBucketOrd, categorizer);
                    }
                    long previousSize = categorizer.ramBytesUsed();
                    LogGroup lg = categorizer.parseLogLine(tokens.toArray(BytesRef[]::new), docCountProvider.getDocCount(doc));
                    long newSize = categorizer.ramBytesUsed();
                    if (newSize - previousSize > 0) {
                        addRequestCircuitBreakerBytes(newSize - previousSize);
                    }

                    long bucketOrd = bucketOrds.add(owningBucketOrd, lg.getId());
                    if (bucketOrd < 0) { // already seen
                        bucketOrd = -1 - bucketOrd;
                        collectExistingBucket(sub, doc, bucketOrd);
                    } else {
                        lg.bucketOrd = bucketOrd;
                        collectBucket(sub, doc, bucketOrd);
                    }
                } finally {
                    ts.close();
                }
            }
        };
    }
}
