/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasables;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.xpack.ml.aggs.categorization.CategorizeTextAggregationBuilder.MAX_MAX_MATCHED_TOKENS;

public class CategorizeTextAggregator extends DeferableBucketAggregator {

    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final SourceLookup sourceLookup;
    private final MappedFieldType fieldType;
    private final CategorizationAnalyzer analyzer;
    private final String sourceFieldName;
    private ObjectArray<CategorizationTokenTree> categorizers;
    private final int maxUniqueTokens;
    private final int maxMatchTokens;
    private final int similarityThreshold;
    private final LongKeyedBucketOrds bucketOrds;
    private final CategorizationBytesRefHash bytesRefHash;

    protected CategorizeTextAggregator(
        String name,
        AggregatorFactories factories,
        AggregationContext context,
        Aggregator parent,
        String sourceFieldName,
        MappedFieldType fieldType,
        TermsAggregator.BucketCountThresholds bucketCountThresholds,
        int maxUniqueTokens,
        int maxMatchTokens,
        int similarityThreshold,
        CategorizationAnalyzerConfig categorizationAnalyzerConfig,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, metadata);
        this.sourceLookup = context.lookup().source();
        this.sourceFieldName = sourceFieldName;
        this.fieldType = fieldType;
        CategorizationAnalyzerConfig analyzerConfig = Optional.ofNullable(categorizationAnalyzerConfig)
            .orElse(CategorizationAnalyzerConfig.buildStandardCategorizationAnalyzer(Collections.emptyList()));
        final String analyzerName = analyzerConfig.getAnalyzer();
        if (analyzerName != null) {
            Analyzer globalAnalyzer = context.getNamedAnalyzer(analyzerName);
            if (globalAnalyzer == null) {
                throw new IllegalArgumentException("Failed to find global analyzer [" + analyzerName + "]");
            }
            this.analyzer = new CategorizationAnalyzer(globalAnalyzer, false);
        } else {
            this.analyzer = new CategorizationAnalyzer(
                context.buildCustomAnalyzer(
                    context.getIndexSettings(),
                    false,
                    analyzerConfig.getTokenizer(),
                    analyzerConfig.getCharFilters(),
                    analyzerConfig.getTokenFilters()
                ),
                true
            );
        }
        this.categorizers = bigArrays().newObjectArray(1);
        this.maxUniqueTokens = maxUniqueTokens;
        this.maxMatchTokens = maxMatchTokens;
        this.similarityThreshold = similarityThreshold;
        this.bucketOrds = LongKeyedBucketOrds.build(bigArrays(), CardinalityUpperBound.MANY);
        this.bucketCountThresholds = bucketCountThresholds;
        this.bytesRefHash = new CategorizationBytesRefHash(new BytesRefHash(2048, bigArrays()));
    }

    @Override
    protected void doClose() {
        super.doClose();
        Releasables.close(this.analyzer, this.bytesRefHash, this.bucketOrds, this.categorizers);
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] ordsToCollect) throws IOException {
        InternalCategorizationAggregation.Bucket[][] topBucketsPerOrd =
            new InternalCategorizationAggregation.Bucket[ordsToCollect.length][];
        for (int ordIdx = 0; ordIdx < ordsToCollect.length; ordIdx++) {
            final CategorizationTokenTree categorizationTokenTree = categorizers.get(ordsToCollect[ordIdx]);
            if (categorizationTokenTree == null) {
                topBucketsPerOrd[ordIdx] = new InternalCategorizationAggregation.Bucket[0];
                continue;
            }
            int size = (int) Math.min(bucketOrds.bucketsInOrd(ordIdx), bucketCountThresholds.getShardSize());
            PriorityQueue<InternalCategorizationAggregation.Bucket> ordered =
                new InternalCategorizationAggregation.BucketCountPriorityQueue(size);
            for (InternalCategorizationAggregation.Bucket bucket : categorizationTokenTree.toIntermediateBuckets(bytesRefHash)) {
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
                maxUniqueTokens,
                maxMatchTokens,
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
            maxUniqueTokens,
            maxMatchTokens,
            similarityThreshold,
            metadata()
        );
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                categorizers = bigArrays().grow(categorizers, owningBucketOrd + 1);
                CategorizationTokenTree categorizer = categorizers.get(owningBucketOrd);
                if (categorizer == null) {
                    categorizer = new CategorizationTokenTree(maxUniqueTokens, maxMatchTokens, similarityThreshold);
                    addRequestCircuitBreakerBytes(categorizer.ramBytesUsed());
                    categorizers.set(owningBucketOrd, categorizer);
                }
                collectFromSource(doc, owningBucketOrd, categorizer);
            }

            private void collectFromSource(int doc, long owningBucketOrd, CategorizationTokenTree categorizer) throws IOException {
                sourceLookup.setSegmentAndDocument(ctx, doc);
                Iterator<String> itr = sourceLookup.extractRawValuesWithoutCaching(sourceFieldName).stream().map(obj -> {
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
                    processTokenStream(owningBucketOrd, ts, doc, categorizer);
                }
            }

            private void processTokenStream(long owningBucketOrd, TokenStream ts, int doc, CategorizationTokenTree categorizer)
                throws IOException {
                ArrayList<Integer> tokens = new ArrayList<>();
                try (ts) {
                    CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
                    ts.reset();
                    int numTokens = 0;
                    // Only categorize the first MAX_MAX_MATCHED_TOKENS tokens
                    while (ts.incrementToken() && numTokens < MAX_MAX_MATCHED_TOKENS) {
                        if (termAtt.length() > 0) {
                            tokens.add(bytesRefHash.put(new BytesRef(termAtt)));
                            numTokens++;
                        }
                    }
                    if (tokens.isEmpty()) {
                        return;
                    }
                }
                long previousSize = categorizer.ramBytesUsed();
                TextCategorization lg = categorizer.parseTokens(
                    tokens.stream().mapToInt(Integer::valueOf).toArray(),
                    docCountProvider.getDocCount(doc)
                );
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
            }
        };
    }
}
