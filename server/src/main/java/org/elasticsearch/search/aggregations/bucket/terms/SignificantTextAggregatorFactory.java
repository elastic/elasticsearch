/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.lucene.analysis.miscellaneous.DeDuplicatingTokenFilter;
import org.elasticsearch.lucene.analysis.miscellaneous.DuplicateByteSequenceSpotter;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude.StringFilter;
import org.elasticsearch.search.aggregations.bucket.terms.MapStringTermsAggregator.CollectConsumer;
import org.elasticsearch.search.aggregations.bucket.terms.MapStringTermsAggregator.CollectorSource;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.search.profile.Timer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.LongConsumer;

public class SignificantTextAggregatorFactory extends AggregatorFactory {
    private static final int MEMORY_GROWTH_REPORTING_INTERVAL_BYTES = 5000;

    private final IncludeExclude includeExclude;
    private final MappedFieldType fieldType;
    private final String[] sourceFieldNames;
    private final QueryBuilder backgroundFilter;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final SignificanceHeuristic significanceHeuristic;
    private final boolean filterDuplicateText;

    public SignificantTextAggregatorFactory(
        String name,
        IncludeExclude includeExclude,
        QueryBuilder backgroundFilter,
        TermsAggregator.BucketCountThresholds bucketCountThresholds,
        SignificanceHeuristic significanceHeuristic,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        String fieldName,
        String[] sourceFieldNames,
        boolean filterDuplicateText,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, subFactoriesBuilder, metadata);

        this.fieldType = context.getFieldType(fieldName);
        if (fieldType != null) {
            if (supportsAgg(fieldType) == false) {
                throw new IllegalArgumentException(
                    "Field [" + fieldType.name() + "] has no analyzer, but SignificantText " + "requires an analyzed field"
                );
            }
            String indexedFieldName = fieldType.name();
            this.sourceFieldNames = sourceFieldNames == null ? new String[] { indexedFieldName } : sourceFieldNames;
        } else {
            this.sourceFieldNames = new String[0];
        }

        this.includeExclude = includeExclude;
        this.backgroundFilter = backgroundFilter;
        this.filterDuplicateText = filterDuplicateText;
        this.bucketCountThresholds = bucketCountThresholds;
        this.significanceHeuristic = significanceHeuristic;
    }

    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        final InternalAggregation aggregation = new UnmappedSignificantTerms(
            name,
            bucketCountThresholds.getRequiredSize(),
            bucketCountThresholds.getMinDocCount(),
            metadata
        );
        return new NonCollectingAggregator(name, context, parent, factories, metadata) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    private static boolean supportsAgg(MappedFieldType ft) {
        return ft.getTextSearchInfo() != TextSearchInfo.NONE && ft.getTextSearchInfo() != TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS;
    }

    @Override
    protected Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {

        if (fieldType == null) {
            return createUnmapped(parent, metadata);
        }

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

        SamplingContext samplingContext = getSamplingContext().orElse(SamplingContext.NONE);
        // If min_doc_count and shard_min_doc_count is provided, we do not support them being larger than 1
        // This is because we cannot be sure about their relative scale when sampled
        if (samplingContext.isSampled()) {
            if ((bucketCountThresholds.getMinDocCount() != SignificantTextAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS
                .getMinDocCount() && bucketCountThresholds.getMinDocCount() > 1)
                || (bucketCountThresholds.getShardMinDocCount() != SignificantTextAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS
                    .getMinDocCount() && bucketCountThresholds.getShardMinDocCount() > 1)) {
                throw new ElasticsearchStatusException(
                    "aggregation [{}] is within a sampling context; "
                        + "min_doc_count, provided [{}], and min_shard_doc_count, provided [{}], cannot be greater than 1",
                    RestStatus.BAD_REQUEST,
                    name(),
                    bucketCountThresholds.getMinDocCount(),
                    bucketCountThresholds.getShardMinDocCount()
                );
            }
        }

        // TODO - need to check with mapping that this is indeed a text field....

        final IncludeExclude.StringFilter incExcFilter = includeExclude == null
            ? null
            : includeExclude.convertToStringFilter(DocValueFormat.RAW);

        final SignificanceLookup lookup = new SignificanceLookup(context, samplingContext, fieldType, DocValueFormat.RAW, backgroundFilter);
        final CollectorSource collectorSource = createCollectorSource();
        boolean success = false;
        try {
            final MapStringTermsAggregator mapStringTermsAggregator = new MapStringTermsAggregator(
                name,
                factories,
                collectorSource,
                a -> a.new SignificantTermsResults(lookup, significanceHeuristic, cardinality),
                null,
                DocValueFormat.RAW,
                bucketCountThresholds,
                incExcFilter,
                context,
                parent,
                SubAggCollectionMode.BREADTH_FIRST,
                false,
                cardinality,
                metadata
            );
            success = true;
            return mapStringTermsAggregator;
        } finally {
            if (success == false) {
                Releasables.close(collectorSource);
            }
        }
    }

    /**
     * Create the {@link CollectorSource}, gathering some timing information
     * if we're profiling.
     * <p>
     * When profiling aggregations {@link LeafBucketCollector#collect(int, long)} method
     * out of the box but our implementation of that method does three things that is
     * useful to get timing for:
     * <ul>
     * <li>Fetch field values from {@code _source}
     * <li>Analyze the field
     * <li>Do all the normal {@code terms} agg stuff with its terms
     * </ul>
     * <p>
     * The most convenient way to measure all of these is to time the fetch and all
     * the normal {@code terms} agg stuff. You can then subtract those timings from
     * the overall collect time to get the analyze time. You can also get the total
     * number of terms that we analyzed by looking at the invocation count on the
     * {@code terms} agg stuff.
     * <p>
     * While we're at it we count the number of values we fetch from source.
     */
    private CollectorSource createCollectorSource() {
        Analyzer analyzer = context.getIndexAnalyzer(f -> { throw new IllegalArgumentException("No analyzer configured for field " + f); });
        String[] fieldNames = Arrays.stream(this.sourceFieldNames)
            .flatMap(sourceFieldName -> context.sourcePath(sourceFieldName).stream())
            .toArray(String[]::new);
        if (context.profiling()) {
            return new ProfilingSignificantTextCollectorSource(
                context.lookup().source(),
                context.bigArrays(),
                fieldType,
                analyzer,
                fieldNames,
                filterDuplicateText
            );
        }
        return new SignificantTextCollectorSource(
            context.lookup().source(),
            context.bigArrays(),
            fieldType,
            analyzer,
            fieldNames,
            filterDuplicateText
        );
    }

    private static class SignificantTextCollectorSource implements MapStringTermsAggregator.CollectorSource {
        private final SourceLookup sourceLookup;
        private final BigArrays bigArrays;
        private final MappedFieldType fieldType;
        private final Analyzer analyzer;
        private final String[] sourceFieldNames;
        private final BytesRefBuilder scratch = new BytesRefBuilder();
        private ObjectArray<DuplicateByteSequenceSpotter> dupSequenceSpotters;

        SignificantTextCollectorSource(
            SourceLookup sourceLookup,
            BigArrays bigArrays,
            MappedFieldType fieldType,
            Analyzer analyzer,
            String[] sourceFieldNames,
            boolean filterDuplicateText
        ) {
            this.sourceLookup = sourceLookup;
            this.bigArrays = bigArrays;
            this.fieldType = fieldType;
            this.analyzer = analyzer;
            this.sourceFieldNames = sourceFieldNames;
            dupSequenceSpotters = filterDuplicateText ? bigArrays.newObjectArray(1) : null;
        }

        @Override
        public String describe() {
            return "analyze " + fieldType.name() + " from _source";
        }

        @Override
        public void collectDebugInfo(BiConsumer<String, Object> add) {}

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
                            Iterator<String> itr = extractRawValues(sourceField).stream().map(obj -> {
                                if (obj == null) {
                                    return null;
                                }
                                if (obj instanceof BytesRef) {
                                    return fieldType.valueForDisplay(obj).toString();
                                }
                                return obj.toString();
                            }).iterator();
                            while (itr.hasNext()) {
                                String text = itr.next();
                                TokenStream ts = analyzer.tokenStream(fieldType.name(), text);
                                processTokenStream(
                                    includeExclude,
                                    doc,
                                    owningBucketOrd,
                                    text,
                                    ts,
                                    inDocTerms,
                                    spotter,
                                    addRequestCircuitBreakerBytes,
                                    sub,
                                    consumer
                                );
                            }
                        }
                    } finally {
                        Releasables.close(inDocTerms);
                    }
                }
            };
        }

        protected void processTokenStream(
            StringFilter includeExclude,
            int doc,
            long owningBucketOrd,
            String text,
            TokenStream ts,
            BytesRefHash inDocTerms,
            DuplicateByteSequenceSpotter spotter,
            LongConsumer addRequestCircuitBreakerBytes,
            LeafBucketCollector sub,
            CollectConsumer consumer
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
                    if (includeExclude != null && false == includeExclude.accept(bytes)) {
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

        /**
         * Extract values from {@code _source}.
         */
        protected List<Object> extractRawValues(String field) {
            return sourceLookup.extractRawValuesWithoutCaching(field);
        }

        @Override
        public void close() {
            Releasables.close(dupSequenceSpotters);
        }
    }

    private static class ProfilingSignificantTextCollectorSource extends SignificantTextCollectorSource {
        private final Timer extract = new Timer();
        private final Timer collectAnalyzed = new Timer();
        private long valuesFetched;
        private long charsFetched;

        private ProfilingSignificantTextCollectorSource(
            SourceLookup sourceLookup,
            BigArrays bigArrays,
            MappedFieldType fieldType,
            Analyzer analyzer,
            String[] sourceFieldNames,
            boolean filterDuplicateText
        ) {
            super(sourceLookup, bigArrays, fieldType, analyzer, sourceFieldNames, filterDuplicateText);
        }

        @Override
        protected void processTokenStream(
            StringFilter includeExclude,
            int doc,
            long owningBucketOrd,
            String text,
            TokenStream ts,
            BytesRefHash inDocTerms,
            DuplicateByteSequenceSpotter spotter,
            LongConsumer addRequestCircuitBreakerBytes,
            LeafBucketCollector sub,
            CollectConsumer consumer
        ) throws IOException {
            valuesFetched++;
            charsFetched += text.length();
            super.processTokenStream(
                includeExclude,
                doc,
                owningBucketOrd,
                text,
                ts,
                inDocTerms,
                spotter,
                addRequestCircuitBreakerBytes,
                sub,
                (subCollector, d, o, bytes) -> {
                    collectAnalyzed.start();
                    try {
                        consumer.accept(subCollector, d, o, bytes);
                    } finally {
                        collectAnalyzed.stop();
                    }
                }
            );
        }

        @Override
        protected List<Object> extractRawValues(String field) {
            extract.start();
            try {
                return super.extractRawValues(field);
            } finally {
                extract.stop();
            }
        }

        @Override
        public void collectDebugInfo(BiConsumer<String, Object> add) {
            super.collectDebugInfo(add);
            add.accept("extract_ns", extract.getApproximateTiming());
            add.accept("extract_count", extract.getCount());
            add.accept("collect_analyzed_ns", collectAnalyzed.getApproximateTiming());
            add.accept("collect_analyzed_count", collectAnalyzed.getCount());
            add.accept("values_fetched", valuesFetched);
            add.accept("chars_fetched", charsFetched);
        }
    }
}
