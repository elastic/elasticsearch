/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
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
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.ml.aggs.categorization.InternalCategorizationAggregation.Bucket;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CategorizeTextAggregator extends DeferableBucketAggregator {

    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final SourceProvider sourceProvider;
    private final SourceFilter sourceFilter;
    private final MappedFieldType fieldType;
    private final CategorizationAnalyzer analyzer;
    private final String sourceFieldName;
    private ObjectArray<TokenListCategorizer> categorizers;
    private final int similarityThreshold;
    private final LongKeyedBucketOrds bucketOrds;
    private final CategorizationBytesRefHash bytesRefHash;
    private final CategorizationPartOfSpeechDictionary partOfSpeechDictionary;

    protected CategorizeTextAggregator(
        String name,
        AggregatorFactories factories,
        AggregationContext context,
        Aggregator parent,
        String sourceFieldName,
        MappedFieldType fieldType,
        TermsAggregator.BucketCountThresholds bucketCountThresholds,
        int similarityThreshold,
        CategorizationAnalyzerConfig categorizationAnalyzerConfig,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, metadata);
        this.sourceProvider = context.lookup();
        this.sourceFieldName = sourceFieldName;
        this.sourceFilter = new SourceFilter(new String[] { sourceFieldName }, Strings.EMPTY_ARRAY);
        this.fieldType = fieldType;
        CategorizationAnalyzerConfig analyzerConfig = Optional.ofNullable(categorizationAnalyzerConfig)
            .orElse(CategorizationAnalyzerConfig.buildStandardCategorizationAnalyzer(List.of()));
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
        this.similarityThreshold = similarityThreshold;
        this.bucketOrds = LongKeyedBucketOrds.build(bigArrays(), CardinalityUpperBound.MANY);
        this.bucketCountThresholds = bucketCountThresholds;
        this.bytesRefHash = new CategorizationBytesRefHash(new BytesRefHash(2048, bigArrays()));
        // TODO: make it possible to choose a language instead of or as well as English for the part-of-speech dictionary
        this.partOfSpeechDictionary = CategorizationPartOfSpeechDictionary.getInstance();
    }

    @Override
    protected void doClose() {
        super.doClose();
        Releasables.close(this.analyzer, this.bytesRefHash, this.bucketOrds, this.categorizers);
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] ordsToCollect) throws IOException {
        Bucket[][] topBucketsPerOrd = new Bucket[ordsToCollect.length][];
        for (int ordIdx = 0; ordIdx < ordsToCollect.length; ordIdx++) {
            final TokenListCategorizer categorizer = categorizers.get(ordsToCollect[ordIdx]);
            if (categorizer == null) {
                topBucketsPerOrd[ordIdx] = new Bucket[0];
                continue;
            }
            int size = (int) Math.min(bucketOrds.bucketsInOrd(ordIdx), bucketCountThresholds.getShardSize());
            topBucketsPerOrd[ordIdx] = categorizer.toOrderedBuckets(size);
        }
        buildSubAggsForAllBuckets(topBucketsPerOrd, Bucket::getBucketOrd, Bucket::setAggregations);
        InternalAggregation[] results = new InternalAggregation[ordsToCollect.length];
        for (int ordIdx = 0; ordIdx < ordsToCollect.length; ordIdx++) {
            results[ordIdx] = new InternalCategorizationAggregation(
                name,
                bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(),
                similarityThreshold,
                metadata(),
                Arrays.asList(topBucketsPerOrd[ordIdx])
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
            similarityThreshold,
            metadata()
        );
    }

    @Override
    protected LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                categorizers = bigArrays().grow(categorizers, owningBucketOrd + 1);
                TokenListCategorizer categorizer = categorizers.get(owningBucketOrd);
                if (categorizer == null) {
                    categorizer = new TokenListCategorizer(bytesRefHash, partOfSpeechDictionary, (float) similarityThreshold / 100.0f);
                    addRequestCircuitBreakerBytes(categorizer.ramBytesUsed());
                    categorizers.set(owningBucketOrd, categorizer);
                }
                collectFromSource(doc, owningBucketOrd, categorizer);
            }

            private void collectFromSource(int doc, long owningBucketOrd, TokenListCategorizer categorizer) throws IOException {
                Source source = sourceProvider.getSource(aggCtx.getLeafReaderContext(), doc).filter(sourceFilter);
                Iterator<String> itr = XContentMapValues.extractRawValues(sourceFieldName, source.source()).stream().map(obj -> {
                    if (obj instanceof BytesRef) {
                        return fieldType.valueForDisplay(obj).toString();
                    }
                    return (obj == null) ? null : obj.toString();
                }).iterator();
                while (itr.hasNext()) {
                    String string = itr.next();
                    try (TokenStream ts = analyzer.tokenStream(fieldType.name(), string)) {
                        processTokenStream(owningBucketOrd, ts, string.length(), doc, categorizer);
                    }
                }
            }

            private void processTokenStream(
                long owningBucketOrd,
                TokenStream ts,
                int unfilteredLength,
                int doc,
                TokenListCategorizer categorizer
            ) throws IOException {
                long previousSize = categorizer.ramBytesUsed();
                TokenListCategory category = categorizer.computeCategory(ts, unfilteredLength, docCountProvider.getDocCount(doc));
                if (category == null) {
                    return;
                }
                long sizeDiff = categorizer.ramBytesUsed() - previousSize;
                addRequestCircuitBreakerBytes(sizeDiff);
                long bucketOrd = bucketOrds.add(owningBucketOrd, category.getId());
                if (bucketOrd < 0) { // already seen
                    collectExistingBucket(sub, doc, -1 - bucketOrd);
                } else {
                    category.setBucketOrd(bucketOrd);
                    collectBucket(sub, doc, bucketOrd);
                }
            }
        };
    }
}
