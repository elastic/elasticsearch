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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.index.FilterableTermsEnum;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;

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

    private final QueryShardContext context;
    private final MappedFieldType fieldType;
    private final DocValueFormat format;
    private final Query backgroundFilter;
    private final int supersetNumDocs;
    private TermsEnum termsEnum;

    SignificanceLookup(QueryShardContext context, MappedFieldType fieldType, DocValueFormat format, QueryBuilder backgroundFilter)
        throws IOException {
        this.context = context;
        this.fieldType = fieldType;
        this.format = format;
        this.backgroundFilter = backgroundFilter == null ? null : backgroundFilter.toQuery(context);
        /*
         * We need to use a superset size that includes deleted docs or we
         * could end up blowing up with bad statistics that cause us to blow
         * up later on.
         */
        IndexSearcher searcher = context.searcher();
        supersetNumDocs = backgroundFilter == null ? searcher.getIndexReader().maxDoc() : searcher.count(this.backgroundFilter);
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
    BackgroundFrequencyForBytes bytesLookup(BigArrays bigArrays, boolean collectsFromSingleBucket) {
        if (collectsFromSingleBucket) {
            return new BackgroundFrequencyForBytes() {
                @Override
                public long freq(BytesRef term) throws IOException {
                    return getBackgroundFrequency(term);
                }

                @Override
                public void close() {}
            };
        }
        return new BackgroundFrequencyForBytes() {
            private final BytesRefHash termToPosition = new BytesRefHash(1, bigArrays);
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
    }

    /**
     * Get the background frequency of a {@link BytesRef} term.
     */
    private long getBackgroundFrequency(BytesRef term) throws IOException {
        return getBackgroundFrequency(fieldType.termQuery(format.format(term).toString(), context));
    }

    /**
     * Get the background frequency of a {@code long} term.
     */
    BackgroundFrequencyForLong longLookup(BigArrays bigArrays, boolean collectsFromSingleBucket) {
        if (collectsFromSingleBucket) {
            return new BackgroundFrequencyForLong() {
                @Override
                public long freq(long term) throws IOException {
                    return getBackgroundFrequency(term);
                }

                @Override
                public void close() {}
            };
        }
        return new BackgroundFrequencyForLong() {
            private final LongHash termToPosition = new LongHash(1, bigArrays);
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
    }

    /**
     * Get the background frequency of a {@code long} term.
     */
    private long getBackgroundFrequency(long term) throws IOException {
        return getBackgroundFrequency(fieldType.termQuery(format.format(term).toString(), context));
    }

    private long getBackgroundFrequency(Query query) throws IOException {
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
        IndexReader reader = context.getIndexReader();
        termsEnum = new FilterableTermsEnum(reader, fieldType.name(), PostingsEnum.NONE, backgroundFilter);
        return termsEnum;
    }

}
