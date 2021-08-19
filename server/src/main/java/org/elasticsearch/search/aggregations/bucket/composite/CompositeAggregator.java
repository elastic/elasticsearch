/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.queries.SearchAfterSortedDocQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.comparators.LongComparator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.RoaringDocIdSet;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.MultiBucketCollector;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

import static org.elasticsearch.search.aggregations.MultiBucketConsumerService.MAX_BUCKET_SETTING;

final class CompositeAggregator extends BucketsAggregator {
    private final int size;
    private final List<String> sourceNames;
    private final int[] reverseMuls;
    private final List<DocValueFormat> formats;
    private final CompositeKey rawAfterKey;

    private final CompositeValuesSourceConfig[] sourceConfigs;
    private final SingleDimensionValuesSource<?>[] sources;
    private final CompositeValuesCollectorQueue queue;

    private final List<Entry> entries = new ArrayList<>();
    private LeafReaderContext currentLeaf;
    private RoaringDocIdSet.Builder docIdSetBuilder;
    private BucketCollector deferredCollectors;

    private boolean earlyTerminated;

    CompositeAggregator(String name, AggregatorFactories factories, AggregationContext context, Aggregator parent,
                        Map<String, Object> metadata,
                        int size, CompositeValuesSourceConfig[] sourceConfigs, CompositeKey rawAfterKey) throws IOException {
        super(name, factories, context, parent, CardinalityUpperBound.MANY, metadata);
        this.size = size;
        this.sourceNames = Arrays.stream(sourceConfigs).map(CompositeValuesSourceConfig::name).collect(Collectors.toList());
        this.reverseMuls = Arrays.stream(sourceConfigs).mapToInt(CompositeValuesSourceConfig::reverseMul).toArray();
        this.formats = Arrays.stream(sourceConfigs).map(CompositeValuesSourceConfig::format).collect(Collectors.toList());
        this.sources = new SingleDimensionValuesSource<?>[sourceConfigs.length];
        // check that the provided size is not greater than the search.max_buckets setting
        int bucketLimit = context.multiBucketConsumer().getLimit();
        if (size > bucketLimit) {
            throw new MultiBucketConsumerService.TooManyBucketsException("Trying to create too many buckets. Must be less than or equal" +
                " to: [" + bucketLimit + "] but was [" + size + "]. This limit can be set by changing the [" + MAX_BUCKET_SETTING.getKey() +
                "] cluster level setting.", bucketLimit);
        }
        this.sourceConfigs = sourceConfigs;
        for (int i = 0; i < sourceConfigs.length; i++) {
            this.sources[i] = sourceConfigs[i].createValuesSource(
                context.bigArrays(),
                context.searcher().getIndexReader(),
                size,
                this::addRequestCircuitBreakerBytes
            );
        }
        this.queue = new CompositeValuesCollectorQueue(context.bigArrays(), sources, size);
        if (rawAfterKey != null) {
            try {
                this.queue.setAfterKey(rawAfterKey);
            } catch (IllegalArgumentException ex) {
                throw new ElasticsearchParseException("Cannot set after key in the composite aggregation [" + name + "] - " +
                    ex.getMessage(), ex);
            }
        }
        this.rawAfterKey = rawAfterKey;
    }

    @Override
    protected void doClose() {
        try {
            Releasables.close(queue);
        } finally {
            Releasables.close(sources);
        }
    }

    @Override
    protected void doPreCollection() throws IOException {
        deferredCollectors = MultiBucketCollector.wrap(false, Arrays.asList(subAggregators));
        collectableSubAggregators = BucketCollector.NO_OP_COLLECTOR;
    }

    @Override
    protected void doPostCollection() throws IOException {
        finishLeaf();
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        // Composite aggregator must be at the top of the aggregation tree
        assert owningBucketOrds.length == 1 && owningBucketOrds[0] == 0L;
        if (deferredCollectors != NO_OP_COLLECTOR) {
            // Replay all documents that contain at least one top bucket (collected during the first pass).
            runDeferredCollections();
        }

        int num = Math.min(size, queue.size());
        final InternalComposite.InternalBucket[] buckets = new InternalComposite.InternalBucket[num];
        long[] bucketOrdsToCollect = new long[queue.size()];
        for (int i = 0; i < queue.size(); i++) {
            bucketOrdsToCollect[i] = i;
        }
        InternalAggregations[] subAggsForBuckets = buildSubAggsForBuckets(bucketOrdsToCollect);
        while (queue.size() > 0) {
            int slot = queue.pop();
            CompositeKey key = queue.toCompositeKey(slot);
            InternalAggregations aggs = subAggsForBuckets[slot];
            long docCount = queue.getDocCount(slot);
            buckets[queue.size()] = new InternalComposite.InternalBucket(sourceNames, formats, key, reverseMuls, docCount, aggs);
        }
        CompositeKey lastBucket = num > 0 ? buckets[num-1].getRawKey() : null;
        return new InternalAggregation[] {
            new InternalComposite(name, size, sourceNames, formats, Arrays.asList(buckets), lastBucket, reverseMuls,
                    earlyTerminated, metadata())
        };
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalComposite(name, size, sourceNames, formats, Collections.emptyList(), null, reverseMuls,
            false, metadata());
    }

    private void finishLeaf() {
        if (currentLeaf != null) {
            DocIdSet docIdSet = docIdSetBuilder.build();
            entries.add(new Entry(currentLeaf, docIdSet));
            currentLeaf = null;
            docIdSetBuilder = null;
        }
    }

    /** Return true if the provided field may have multiple values per document in the leaf **/
    private boolean isMaybeMultivalued(LeafReaderContext context, SortField sortField) throws IOException {
        SortField.Type type = IndexSortConfig.getSortFieldType(sortField);
        switch (type) {
            case STRING:
                final SortedSetDocValues v1 = context.reader().getSortedSetDocValues(sortField.getField());
                return v1 != null && DocValues.unwrapSingleton(v1) == null;

            case DOUBLE:
            case FLOAT:
            case LONG:
            case INT:
                final SortedNumericDocValues v2 = context.reader().getSortedNumericDocValues(sortField.getField());
                return v2 != null && DocValues.unwrapSingleton(v2) == null;

            default:
                // we have no clue whether the field is multi-valued or not so we assume it is.
                return true;
        }
    }

    /**
     * Returns the {@link Sort} prefix that is eligible to index sort
     * optimization and null if index sort is not applicable.
     */
    private Sort buildIndexSortPrefix(LeafReaderContext context) throws IOException {
        Sort indexSort = context.reader().getMetaData().getSort();
        if (indexSort == null) {
            return null;
        }
        List<SortField> sortFields = new ArrayList<>();
        int end = Math.min(indexSort.getSort().length, sourceConfigs.length);
        for (int i = 0; i < end; i++) {
            CompositeValuesSourceConfig sourceConfig = sourceConfigs[i];
            SingleDimensionValuesSource<?> source = sources[i];
            SortField indexSortField = indexSort.getSort()[i];
            if (source.fieldType == null
                    // TODO: can we handle missing bucket when using index sort optimization ?
                    || source.missingBucket.include()
                    || indexSortField.getField().equals(source.fieldType.name()) == false
                    || isMaybeMultivalued(context, indexSortField)
                    || sourceConfig.hasScript()) {
                break;
            }

            if (indexSortField.getReverse() != (source.reverseMul == -1)) {
                if (i == 0) {
                    // the leading index sort matches the leading source field but the order is reversed
                    // so we don't check the other sources.
                    return new Sort(indexSortField);
                }
                break;
            }
            sortFields.add(indexSortField);
            if (sourceConfig.valuesSource() instanceof RoundingValuesSource) {
                // the rounding "squashes" many values together, that breaks the ordering of sub-values
                // so we ignore subsequent source even if they match the index sort.
                break;
            }
        }
        return sortFields.isEmpty() ? null : new Sort(sortFields.toArray(new SortField[0]));
    }

    /**
     * Return the number of leading sources that match the index sort.
     *
     * @param indexSortPrefix The index sort prefix that matches the sources
     * @return The length of the index sort prefix if the sort order matches
     *         or -1 if the leading index sort is in the reverse order of the
     *         leading source. A value of 0 indicates that the index sort is
     *         not applicable.
     */
    private int computeSortPrefixLen(Sort indexSortPrefix) {
        if (indexSortPrefix == null) {
            return 0;
        }
        if (indexSortPrefix.getSort()[0].getReverse() != (sources[0].reverseMul == -1)) {
            assert indexSortPrefix.getSort().length == 1;
            return -1;
        } else {
            return indexSortPrefix.getSort().length;
        }
    }

    /**
     * Rewrites the provided {@link Sort} to apply rounding on {@link SortField} that target
     * {@link RoundingValuesSource}.
     */
    private Sort applySortFieldRounding(Sort sort) {
        SortField[] sortFields = new SortField[sort.getSort().length];
        for (int i = 0; i < sort.getSort().length; i++) {
            if (sourceConfigs[i].valuesSource() instanceof RoundingValuesSource) {
                LongUnaryOperator round = ((RoundingValuesSource) sourceConfigs[i].valuesSource())::round;
                final SortedNumericSortField delegate = (SortedNumericSortField) sort.getSort()[i];
                sortFields[i] = new SortedNumericSortField(delegate.getField(), delegate.getNumericType(), delegate.getReverse()) {
                    @Override
                    public boolean equals(Object obj) {
                        return delegate.equals(obj);
                    }

                    @Override
                    public int hashCode() {
                        return delegate.hashCode();
                    }

                    @Override
                    public FieldComparator<?> getComparator(int numHits, int sortPos) {
                        return new LongComparator(1, delegate.getField(), (Long) missingValue, delegate.getReverse(), sortPos) {
                            @Override
                            public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
                                return new LongLeafComparator(context) {
                                    @Override
                                    protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field)
                                            throws IOException {
                                        NumericDocValues dvs =  SortedNumericSelector.wrap(
                                                DocValues.getSortedNumeric(context.reader(), field),
                                            delegate.getSelector(), delegate.getNumericType());
                                        return new NumericDocValues() {
                                            @Override
                                            public long longValue() throws IOException {
                                                return round.applyAsLong(dvs.longValue());
                                            }

                                            @Override
                                            public boolean advanceExact(int target) throws IOException {
                                                return dvs.advanceExact(target);
                                            }

                                            @Override
                                            public int docID() {
                                                return dvs.docID();
                                            }

                                            @Override
                                            public int nextDoc() throws IOException {
                                                return dvs.nextDoc();
                                            }

                                            @Override
                                            public int advance(int target) throws IOException {
                                                return dvs.advance(target);
                                            }

                                            @Override
                                            public long cost() {
                                                return dvs.cost();
                                            }
                                        };
                                    }
                                };
                            }
                        };
                    }
                };
            } else {
                sortFields[i] = sort.getSort()[i];
            }
        }
        return new Sort(sortFields);
    }

    private void processLeafFromQuery(LeafReaderContext ctx, Sort indexSortPrefix) throws IOException {
        DocValueFormat[] formats = new DocValueFormat[indexSortPrefix.getSort().length];
        for (int i = 0; i < formats.length; i++) {
            formats[i] = sources[i].format;
        }
        FieldDoc fieldDoc = SearchAfterBuilder.buildFieldDoc(new SortAndFormats(indexSortPrefix, formats),
            Arrays.copyOfRange(rawAfterKey.values(), 0, formats.length), null);
        if (indexSortPrefix.getSort().length < sources.length) {
            // include all docs that belong to the partial bucket
            fieldDoc.doc = -1;
        }
        BooleanQuery newQuery = new BooleanQuery.Builder()
            .add(topLevelQuery(), BooleanClause.Occur.MUST)
            .add(new SearchAfterSortedDocQuery(applySortFieldRounding(indexSortPrefix), fieldDoc), BooleanClause.Occur.FILTER)
            .build();
        Weight weight = searcher().createWeight(searcher().rewrite(newQuery), ScoreMode.COMPLETE_NO_SCORES, 1f);
        Scorer scorer = weight.scorer(ctx);
        if (scorer != null) {
            DocIdSetIterator docIt = scorer.iterator();
            final LeafBucketCollector inner = queue.getLeafCollector(ctx,
                getFirstPassCollector(docIdSetBuilder, indexSortPrefix.getSort().length));
            inner.setScorer(scorer);
            final Bits liveDocs = ctx.reader().getLiveDocs();
            while (docIt.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                if (liveDocs == null || liveDocs.get(docIt.docID())) {
                    inner.collect(docIt.docID());
                }
            }
        }
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        finishLeaf();

        boolean fillDocIdSet = deferredCollectors != NO_OP_COLLECTOR;

        Sort indexSortPrefix = buildIndexSortPrefix(ctx);
        int sortPrefixLen = computeSortPrefixLen(indexSortPrefix);

        SortedDocsProducer sortedDocsProducer = (sortPrefixLen == 0 && parent == null) ?
            sources[0].createSortedDocsProducerOrNull(ctx.reader(), topLevelQuery()) : null;
        if (sortedDocsProducer != null) {
            // Visit documents sorted by the leading source of the composite definition and terminates
            // when the leading source value is guaranteed to be greater than the lowest composite bucket
            // in the queue.
            DocIdSet docIdSet = sortedDocsProducer.processLeaf(topLevelQuery(), queue, ctx, fillDocIdSet);
            if (fillDocIdSet) {
                entries.add(new Entry(ctx, docIdSet));
            }
            // We can bypass search entirely for this segment, the processing is done in the previous call.
            // Throwing this exception will terminate the execution of the search for this root aggregation,
            // see {@link MultiCollector} for more details on how we handle early termination in aggregations.
            earlyTerminated = true;
            return LeafBucketCollector.NO_OP_COLLECTOR;
        } else {
            if (fillDocIdSet) {
                currentLeaf = ctx;
                docIdSetBuilder = new RoaringDocIdSet.Builder(ctx.reader().maxDoc());
            }
            if (rawAfterKey != null && sortPrefixLen > 0) {
                // We have an after key and index sort is applicable so we jump directly to the doc
                // that is after the index sort prefix using the rawAfterKey and we start collecting
                // document from there.
                try {
                    processLeafFromQuery(ctx, indexSortPrefix);
                } catch (CollectionTerminatedException e) {
                    /*
                     * Signal that there isn't anything to collect. We're going
                     * to return noop collector anyway so we can ignore it.
                     */
                }
                return LeafBucketCollector.NO_OP_COLLECTOR;
            } else {
                final LeafBucketCollector inner;
                try {
                    inner = queue.getLeafCollector(ctx, getFirstPassCollector(docIdSetBuilder, sortPrefixLen));
                } catch (CollectionTerminatedException e) {
                    return LeafBucketCollector.NO_OP_COLLECTOR;
                }
                return new LeafBucketCollector() {
                    @Override
                    public void collect(int doc, long zeroBucket) throws IOException {
                        assert zeroBucket == 0L;
                        inner.collect(doc);
                    }
                };
            }
        }
    }

    /**
     * The first pass selects the top composite buckets from all matching documents.
     */
    private LeafBucketCollector getFirstPassCollector(RoaringDocIdSet.Builder builder, int indexSortPrefix) {
        return new LeafBucketCollector() {
            int lastDoc = -1;

            @Override
            public void collect(int doc, long bucket) throws IOException {
                try {
                    int docCount = docCountProvider.getDocCount(doc);
                    if (queue.addIfCompetitive(indexSortPrefix, docCount)) {
                        if (builder != null && lastDoc != doc) {
                            builder.add(doc);
                            lastDoc = doc;
                        }
                    }
                } catch (CollectionTerminatedException exc) {
                    earlyTerminated = true;
                    throw exc;
                }
            }
        };
    }

    /**
     * Replay the documents that might contain a top bucket and pass top buckets to
     * the {@link #deferredCollectors}.
     */
    private void runDeferredCollections() throws IOException {
        final boolean needsScores = scoreMode().needsScores();
        Weight weight = null;
        if (needsScores) {
            weight = searcher().createWeight(searcher().rewrite(topLevelQuery()), ScoreMode.COMPLETE, 1f);
        }
        deferredCollectors.preCollection();
        for (Entry entry : entries) {
            DocIdSetIterator docIdSetIterator = entry.docIdSet.iterator();
            if (docIdSetIterator == null) {
                continue;
            }
            final LeafBucketCollector subCollector = deferredCollectors.getLeafCollector(entry.context);
            final LeafBucketCollector collector = queue.getLeafCollector(entry.context, getSecondPassCollector(subCollector));
            DocIdSetIterator scorerIt = null;
            if (needsScores) {
                Scorer scorer = weight.scorer(entry.context);
                if (scorer != null) {
                    scorerIt = scorer.iterator();
                    subCollector.setScorer(scorer);
                }
            }
            int docID;
            while ((docID = docIdSetIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (needsScores) {
                    assert scorerIt != null && scorerIt.docID() < docID;
                    scorerIt.advance(docID);
                    // aggregations should only be replayed on matching documents
                    assert scorerIt.docID() == docID;
                }
                collector.collect(docID);
            }
        }
        deferredCollectors.postCollection();
    }

    /**
     * Replay the top buckets from the matching documents.
     */
    private LeafBucketCollector getSecondPassCollector(LeafBucketCollector subCollector) {
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long zeroBucket) throws IOException {
                assert zeroBucket == 0;
                Integer slot = queue.compareCurrent();
                if (slot != null) {
                    // The candidate key is a top bucket.
                    // We can defer the collection of this document/bucket to the sub collector
                    subCollector.collect(doc, slot);
                }
            }
        };
    }

    private static class Entry {
        final LeafReaderContext context;
        final DocIdSet docIdSet;

        Entry(LeafReaderContext context, DocIdSet docIdSet) {
            this.context = context;
            this.docIdSet = docIdSet;
        }
    }
}

