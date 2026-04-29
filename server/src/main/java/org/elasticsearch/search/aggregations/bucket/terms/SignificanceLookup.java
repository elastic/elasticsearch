/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.index.FilterableTermsEnum;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.search.internal.CancellableBulkScorer;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;

/**
 * Looks up values used for {@link SignificanceHeuristic}s.
 */
class SignificanceLookup {
    /**
     * Lookup frequencies for {@link BytesRef} terms.
     */
    interface BackgroundFrequencyForBytes extends Releasable {
        long freq(BytesRef term) throws IOException;
    }

    /**
     * Lookup frequencies for {@code long} terms.
     */
    interface BackgroundFrequencyForLong extends Releasable {
        long freq(long term) throws IOException;
    }

    private final AggregationContext context;
    private final MappedFieldType fieldType;
    private final DocValueFormat format;
    private final Query backgroundFilter;
    private final int supersetNumDocs;
    private TermsEnum termsEnum;

    SignificanceLookup(
        AggregationContext context,
        SamplingContext samplingContext,
        MappedFieldType fieldType,
        DocValueFormat format,
        QueryBuilder backgroundFilter
    ) throws IOException {
        this.context = context;
        this.fieldType = fieldType;
        this.format = format;
        // If there is no provided background filter, but we are within a sampling context, our background docs need to take the sampling
        // context into account.
        // If there is a filter, that filter needs to take the sampling into account (if we are within a sampling context)
        Query backgroundQuery = backgroundFilter == null
            ? samplingContext.buildSamplingQueryIfNecessary(context).orElse(null)
            : samplingContext.buildQueryWithSampler(backgroundFilter, context);
        // Refilter to account for alias filters, if there are any.
        if (backgroundQuery == null) {
            Query matchAllDocsQuery = Queries.ALL_DOCS_INSTANCE;
            Query contextFiltered = context.filterQuery(matchAllDocsQuery);
            if (contextFiltered != matchAllDocsQuery) {
                this.backgroundFilter = contextFiltered;
            } else {
                this.backgroundFilter = null;
            }
        } else {
            this.backgroundFilter = context.filterQuery(backgroundQuery);
        }
        /*
         * We need to use a superset size that includes deleted docs or we
         * could end up blowing up with bad statistics that cause us to blow
         * up later on.
         */
        IndexSearcher searcher = context.searcher();
        supersetNumDocs = this.backgroundFilter == null ? searcher.getIndexReader().maxDoc() : searcher.count(this.backgroundFilter);
    }

    /**
     * Get the number of docs in the superset.
     */
    long supersetSize() {
        return supersetNumDocs;
    }

    /**
     * Get the background frequency of a {@link BytesRef} term.
     */
    BackgroundFrequencyForBytes bytesLookup(BigArrays bigArrays, CardinalityUpperBound cardinality) {
        if (cardinality == CardinalityUpperBound.ONE) {
            return new BackgroundFrequencyForBytes() {
                @Override
                public long freq(BytesRef term) throws IOException {
                    return getBackgroundFrequency(term);
                }

                @Override
                public void close() {}
            };
        }
        final BytesRefHash termToPosition = new BytesRefHash(1, bigArrays);
        boolean success = false;
        try {
            BackgroundFrequencyForBytes b = new BackgroundFrequencyForBytes() {
                private LongArray positionToFreq = bigArrays.newLongArray(1, false);

                @Override
                public long freq(BytesRef term) throws IOException {
                    long position = termToPosition.add(term);
                    if (position < 0) {
                        return positionToFreq.get(-1 - position);
                    }
                    long freq = getBackgroundFrequency(term);
                    positionToFreq = bigArrays.grow(positionToFreq, position + 1);
                    positionToFreq.set(position, freq);
                    return freq;
                }

                @Override
                public void close() {
                    Releasables.close(termToPosition, positionToFreq);
                }
            };
            success = true;
            return b;
        } finally {
            if (success == false) {
                termToPosition.close();
            }
        }

    }

    /**
     * Get the background frequency of a {@link BytesRef} term.
     */
    private long getBackgroundFrequency(BytesRef term) throws IOException {
        return getBackgroundFrequency(context.buildQuery(makeBackgroundFrequencyQuery(format.format(term).toString())));
    }

    /**
     * Get the background frequency of a {@code long} term.
     */
    BackgroundFrequencyForLong longLookup(BigArrays bigArrays, CardinalityUpperBound cardinality) {
        if (cardinality == CardinalityUpperBound.ONE) {
            return new BackgroundFrequencyForLong() {
                @Override
                public long freq(long term) throws IOException {
                    return getBackgroundFrequency(term);
                }

                @Override
                public void close() {}
            };
        }
        final LongHash termToPosition = new LongHash(1, bigArrays);
        boolean success = false;
        try {
            BackgroundFrequencyForLong b = new BackgroundFrequencyForLong() {

                private LongArray positionToFreq = bigArrays.newLongArray(1, false);

                @Override
                public long freq(long term) throws IOException {
                    long position = termToPosition.add(term);
                    if (position < 0) {
                        return positionToFreq.get(-1 - position);
                    }
                    long freq = getBackgroundFrequency(term);
                    positionToFreq = bigArrays.grow(positionToFreq, position + 1);
                    positionToFreq.set(position, freq);
                    return freq;
                }

                @Override
                public void close() {
                    Releasables.close(termToPosition, positionToFreq);
                }
            };
            success = true;
            return b;
        } finally {
            if (success == false) {
                termToPosition.close();
            }
        }
    }

    /**
     * Get the background frequency of a {@code long} term.
     */
    private long getBackgroundFrequency(long term) throws IOException {
        return getBackgroundFrequency(context.buildQuery(makeBackgroundFrequencyQuery(format.format(term).toString())));
    }

    private QueryBuilder makeBackgroundFrequencyQuery(String value) {
        QueryBuilder queryBuilder = new TermQueryBuilder(fieldType.name(), value);

        var nestedParentField = context.nestedLookup().getNestedParent(fieldType.name());
        if (nestedParentField != null) {
            queryBuilder = new NestedQueryBuilder(nestedParentField, queryBuilder, ScoreMode.Avg);
        }

        return queryBuilder;
    }

    private long getBackgroundFrequency(Query query) throws IOException {
        // Note that `getTermsEnum` takes into account the backgroundFilter, with already has the sampling query applied
        if (query instanceof TermQuery) {
            // for types that use the inverted index, we prefer using a terms
            // enum that will do a better job at reusing index inputs
            Term term = ((TermQuery) query).getTerm();
            TermsEnum termsEnum = getTermsEnum();
            if (termsEnum.seekExact(term.bytes())) {
                return termsEnum.docFreq();
            }
            return 0;
        }
        // otherwise do it the naive way
        if (backgroundFilter != null) {
            query = new BooleanQuery.Builder().add(query, Occur.FILTER).add(backgroundFilter, Occur.FILTER).build();
        }
        // Use a brand new index searcher so this count runs on the current thread, but wrap the
        // bulk scorer with CancellableBulkScorer so long-running scoring (e.g. against a script
        // field that loads synthetic source per doc) honors task cancellation.
        return cancellableSearcher(context.searcher().getIndexReader(), this::checkCancelled).count(query);
    }

    private void checkCancelled() {
        if (context.isCancelled()) {
            throw new TaskCancelledException("cancelled");
        }
    }

    /**
     * Build an {@link IndexSearcher} backed by {@code reader} that runs counts on the current
     * thread and wires {@code checkCancelled} into the bulk-scoring loop, so long-running counts
     * (e.g. against a script field that loads synthetic source per doc) can be interrupted.
     * Visible for testing.
     */
    static IndexSearcher cancellableSearcher(IndexReader reader, Runnable checkCancelled) {
        return new IndexSearcher(reader) {
            @Override
            protected void searchLeaf(LeafReaderContext ctx, int minDocId, int maxDocId, Weight weight, Collector collector)
                throws IOException {
                checkCancelled.run();
                super.searchLeaf(ctx, minDocId, maxDocId, cancellableWeight(weight, checkCancelled), collector);
            }
        };
    }

    /**
     * Wrap {@code delegate} so the {@link BulkScorer} it produces is itself wrapped in a
     * {@link CancellableBulkScorer}. {@link IndexSearcher#searchLeaf} reaches the bulk scorer via
     * {@code weight.scorerSupplier(ctx).bulkScorer()}, so we override {@link Weight#scorerSupplier}
     * and the resulting supplier's {@link ScorerSupplier#bulkScorer} is where the cancellation
     * hook is installed.
     */
    private static Weight cancellableWeight(Weight delegate, Runnable checkCancelled) {
        return new FilterWeight(delegate) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext ctx) throws IOException {
                ScorerSupplier inner = super.scorerSupplier(ctx);
                if (inner == null) {
                    return null;
                }
                return new ScorerSupplier() {
                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        return inner.get(leadCost);
                    }

                    @Override
                    public BulkScorer bulkScorer() throws IOException {
                        BulkScorer bs = inner.bulkScorer();
                        return bs == null ? null : new CancellableBulkScorer(bs, checkCancelled);
                    }

                    @Override
                    public long cost() {
                        return inner.cost();
                    }

                    @Override
                    public void setTopLevelScoringClause() throws IOException {
                        inner.setTopLevelScoringClause();
                    }
                };
            }
        };
    }

    private TermsEnum getTermsEnum() throws IOException {
        // TODO this method helps because of asMultiBucketAggregator. Once we remove it we can move this logic into the aggregators.
        if (termsEnum != null) {
            return termsEnum;
        }
        IndexReader reader = context.searcher().getIndexReader();
        termsEnum = new FilterableTermsEnum(reader, fieldType.name(), PostingsEnum.NONE, backgroundFilter);
        return termsEnum;
    }

}
