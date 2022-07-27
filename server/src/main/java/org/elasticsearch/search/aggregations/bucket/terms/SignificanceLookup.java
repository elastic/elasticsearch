/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.index.FilterableTermsEnum;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.SamplingContext;

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
            Query matchAllDocsQuery = new MatchAllDocsQuery();
            Query contextFiltered = context.filterQuery(matchAllDocsQuery);
            if (contextFiltered != matchAllDocsQuery) {
                this.backgroundFilter = contextFiltered;
            } else {
                this.backgroundFilter = null;
            }
        } else {
            Query contextFiltered = context.filterQuery(backgroundQuery);
            this.backgroundFilter = contextFiltered;
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
        return getBackgroundFrequency(context.buildQuery(new TermQueryBuilder(fieldType.name(), format.format(term).toString())));
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
        return getBackgroundFrequency(context.buildQuery(new TermQueryBuilder(fieldType.name(), format.format(term).toString())));
    }

    private long getBackgroundFrequency(Query query) throws IOException {
        // Note that `getTermsEnum` takes into account the backgroundFilter, with already has the sampling query applied
        if (query instanceof TermQuery) {
            // for types that use the inverted index, we prefer using a terms
            // enum that will do a better job at reusing index inputs
            Term term = ((TermQuery) query).getTerm();
            TermsEnum termsEnum = getTermsEnum(term.field());
            if (termsEnum.seekExact(term.bytes())) {
                return termsEnum.docFreq();
            }
            return 0;
        }
        // otherwise do it the naive way
        if (backgroundFilter != null) {
            query = new BooleanQuery.Builder().add(query, Occur.FILTER).add(backgroundFilter, Occur.FILTER).build();
        }
        return context.searcher().count(query);
    }

    private TermsEnum getTermsEnum(String field) throws IOException {
        // TODO this method helps because of asMultiBucketAggregator. Once we remove it we can move this logic into the aggregators.
        if (termsEnum != null) {
            return termsEnum;
        }
        IndexReader reader = context.searcher().getIndexReader();
        termsEnum = new FilterableTermsEnum(reader, fieldType.name(), PostingsEnum.NONE, backgroundFilter);
        return termsEnum;
    }

}
