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

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.DeDuplicatingTokenFilter;
import org.apache.lucene.analysis.miscellaneous.DuplicateByteSequenceSpotter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude.StringFilter;
import org.elasticsearch.search.aggregations.bucket.terms.MapStringTermsAggregator.CollectConsumer;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.function.LongConsumer;

public class SignificantTextAggregatorFactory extends AggregatorFactory {
    private static final int MEMORY_GROWTH_REPORTING_INTERVAL_BYTES = 5000;

    private final IncludeExclude includeExclude;
    private final String indexedFieldName;
    private final MappedFieldType fieldType;
    private final String[] sourceFieldNames;
    private final QueryBuilder backgroundFilter;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final SignificanceHeuristic significanceHeuristic;
    private final boolean filterDuplicateText;

    public SignificantTextAggregatorFactory(String name,
                                                IncludeExclude includeExclude,
                                                QueryBuilder backgroundFilter,
                                                TermsAggregator.BucketCountThresholds bucketCountThresholds,
                                                SignificanceHeuristic significanceHeuristic,
                                                QueryShardContext queryShardContext,
                                                AggregatorFactory parent,
                                                AggregatorFactories.Builder subFactoriesBuilder,
                                                String fieldName,
                                                String [] sourceFieldNames,
                                                boolean filterDuplicateText,
                                                Map<String, Object> metadata) throws IOException {
        super(name, queryShardContext, parent, subFactoriesBuilder, metadata);

        // Note that if the field is unmapped (its field type is null), we don't fail,
        // and just use the given field name as a placeholder.
        this.fieldType = queryShardContext.fieldMapper(fieldName);
        if (fieldType != null && fieldType.indexAnalyzer() == null) {
            throw new IllegalArgumentException("Field [" + fieldType.name() + "] has no analyzer, but SignificantText " +
                "requires an analyzed field");
        }
        this.indexedFieldName = fieldType != null ? fieldType.name() : fieldName;
        this.sourceFieldNames = sourceFieldNames == null ? new String[] { indexedFieldName } : sourceFieldNames;

        this.includeExclude = includeExclude;
        this.backgroundFilter = backgroundFilter;
        this.filterDuplicateText = filterDuplicateText;
        this.bucketCountThresholds = bucketCountThresholds;
        this.significanceHeuristic = significanceHeuristic;
    }

    @Override
    protected Aggregator createInternal(SearchContext searchContext, Aggregator parent, CardinalityUpperBound cardinality,
                                        Map<String, Object> metadata) throws IOException {
        BucketCountThresholds bucketCountThresholds = new BucketCountThresholds(this.bucketCountThresholds);
        if (bucketCountThresholds.getShardSize() == SignificantTextAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS.getShardSize()) {
            // The user has not made a shardSize selection.
            // Use default heuristic to avoid any wrong-ranking caused by
            // distributed counting but request double the usual amount.
            // We typically need more than the number of "top" terms requested
            // by other aggregations as the significance algorithm is in less
            // of a position to down-select at shard-level - some of the things
            // we want to find have only one occurrence on each shard and as
            // such are impossible to differentiate from non-significant terms
            // at that early stage.
            bucketCountThresholds.setShardSize(2 * BucketUtils.suggestShardSideQueueSize(bucketCountThresholds.getRequiredSize()));
        }

//        TODO - need to check with mapping that this is indeed a text field....

        IncludeExclude.StringFilter incExcFilter = includeExclude == null ? null:
            includeExclude.convertToStringFilter(DocValueFormat.RAW);

        MapStringTermsAggregator.CollectorSource collectorSource = new SignificantTextCollectorSource(
            queryShardContext.lookup().source(),
            queryShardContext.bigArrays(),
            fieldType,
            sourceFieldNames,
            filterDuplicateText
        );
        SignificanceLookup lookup = new SignificanceLookup(queryShardContext, fieldType, DocValueFormat.RAW, backgroundFilter);
        return new MapStringTermsAggregator(
            name,
            factories,
            collectorSource,
            a -> a.new SignificantTermsResults(lookup, significanceHeuristic, cardinality),
            null,
            DocValueFormat.RAW,
            bucketCountThresholds,
            incExcFilter,
            searchContext,
            parent,
            SubAggCollectionMode.BREADTH_FIRST,
            false,
            cardinality,
            metadata
        );
    }

    private static class SignificantTextCollectorSource implements MapStringTermsAggregator.CollectorSource {
        private final SourceLookup sourceLookup;
        private final BigArrays bigArrays;
        private final MappedFieldType fieldType;
        private final String[] sourceFieldNames;
        private ObjectArray<DuplicateByteSequenceSpotter> dupSequenceSpotters;

        SignificantTextCollectorSource(
            SourceLookup sourceLookup,
            BigArrays bigArrays,
            MappedFieldType fieldType,
            String[] sourceFieldNames,
            boolean filterDuplicateText
        ) {
            this.sourceLookup = sourceLookup;
            this.bigArrays = bigArrays;
            this.fieldType = fieldType;
            this.sourceFieldNames = sourceFieldNames;
            dupSequenceSpotters = filterDuplicateText ? bigArrays.newObjectArray(1) : null;
        }

        @Override
        public boolean needsScores() {
            return false;
        }

        @Override
        public LeafBucketCollector getLeafCollector(
            StringFilter includeExclude,
            LeafReaderContext ctx,
            LeafBucketCollector sub,
            LongConsumer addRequestCircuitBreakerBytes,
            CollectConsumer consumer
        ) throws IOException {
            return new LeafBucketCollectorBase(sub, null) {
                private final BytesRefBuilder scratch = new BytesRefBuilder();

                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    if (dupSequenceSpotters == null) {
                        collectFromSource(doc, owningBucketOrd, null);
                        return;
                    }
                    dupSequenceSpotters = bigArrays.grow(dupSequenceSpotters, owningBucketOrd + 1);
                    DuplicateByteSequenceSpotter spotter = dupSequenceSpotters.get(owningBucketOrd);
                    if (spotter == null) {
                        spotter = new DuplicateByteSequenceSpotter();
                        dupSequenceSpotters.set(owningBucketOrd, spotter);
                    }
                    collectFromSource(doc, owningBucketOrd, spotter);
                    spotter.startNewSequence();
                }

                private void collectFromSource(int doc, long owningBucketOrd, DuplicateByteSequenceSpotter spotter) throws IOException {
                    sourceLookup.setSegmentAndDocument(ctx, doc);
                    BytesRefHash inDocTerms = new BytesRefHash(256, bigArrays);

                    try {
                        for (String sourceField : sourceFieldNames) {
                            Iterator<String> itr = sourceLookup.extractRawValues(sourceField).stream()
                                .map(obj -> {
                                    if (obj == null) {
                                        return null;
                                    }
                                    if (obj instanceof BytesRef) {
                                        return fieldType.valueForDisplay(obj).toString();
                                    }
                                    return obj.toString();
                                })
                                .iterator();
                            Analyzer analyzer = fieldType.indexAnalyzer();
                            while (itr.hasNext()) {
                                TokenStream ts = analyzer.tokenStream(fieldType.name(), itr.next());
                                processTokenStream(doc, owningBucketOrd, ts, inDocTerms, spotter);
                            }
                        }
                    } finally {
                        Releasables.close(inDocTerms);
                    }
                }

                private void processTokenStream(
                    int doc,
                    long owningBucketOrd,
                    TokenStream ts,
                    BytesRefHash inDocTerms,
                    DuplicateByteSequenceSpotter spotter
                ) throws IOException {
                    long lastTrieSize = 0;
                    if (spotter != null) {
                        lastTrieSize = spotter.getEstimatedSizeInBytes();
                        ts = new DeDuplicatingTokenFilter(ts, spotter);
                    }
                    CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
                    ts.reset();
                    try {
                        while (ts.incrementToken()) {
                            if (spotter != null) {
                                long newTrieSize = spotter.getEstimatedSizeInBytes();
                                long growth = newTrieSize - lastTrieSize;
                                // Only update the circuitbreaker after
                                if (growth > MEMORY_GROWTH_REPORTING_INTERVAL_BYTES) {
                                    addRequestCircuitBreakerBytes.accept(growth);
                                    lastTrieSize = newTrieSize;
                                }
                            }

                            scratch.clear();
                            scratch.copyChars(termAtt);
                            BytesRef bytes = scratch.get();
                            if (includeExclude != null && includeExclude.accept(bytes)) {
                                continue;
                            }
                            if (inDocTerms.add(bytes) < 0) {
                                continue;
                            }
                            consumer.accept(sub, doc, owningBucketOrd, bytes);
                        }
                    } finally {
                        ts.close();
                    }
                    if (spotter != null) {
                        long growth = spotter.getEstimatedSizeInBytes() - lastTrieSize;
                        if (growth > 0) {
                            addRequestCircuitBreakerBytes.accept(growth);
                        }
                    }
                }
            };
        }

        @Override
        public void close() {
            Releasables.close(dupSequenceSpotters);
        }
    }
}
