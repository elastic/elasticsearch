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
package org.elasticsearch.search.aggregations.bucket.significant;

import org.apache.lucene.index.*;
import org.apache.lucene.index.FilterAtomicReader.FilterTermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.Aggregator.BucketAggregationMode;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueParser;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class SignificantTermsAggregatorFactory extends ValuesSourceAggregatorFactory implements Releasable {

    public static final String EXECUTION_HINT_VALUE_MAP = "map";
    public static final String EXECUTION_HINT_VALUE_ORDINALS = "ordinals";
    static final int INITIAL_NUM_TERM_FREQS_CACHED = 512;

    private final int requiredSize;
    private final int shardSize;
    private final long minDocCount;
    private final IncludeExclude includeExclude;
    private final String executionHint;
    private String indexedFieldName;
    private FieldMapper mapper;
    private IntArray termDocFreqs;
    private BytesRefHash cachedTermOrds;
    private BigArrays bigArrays;
    private TermsEnum termsEnum;
    private int numberOfAggregatorsCreated = 0;

    public SignificantTermsAggregatorFactory(String name, ValuesSourceConfig valueSourceConfig, ValueFormatter formatter, ValueParser parser,
            int requiredSize, int shardSize, long minDocCount, IncludeExclude includeExclude, String executionHint) {

        super(name, SignificantStringTerms.TYPE.name(), valueSourceConfig, formatter, parser);
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        this.minDocCount = minDocCount;
        this.includeExclude = includeExclude;
        this.executionHint = executionHint;
        if (!valueSourceConfig.unmapped()) {
            this.indexedFieldName = config.fieldContext().field();
            mapper = SearchContext.current().smartNameFieldMapper(indexedFieldName);
        }
        bigArrays = SearchContext.current().bigArrays();
    }

    @Override
    protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
        final InternalAggregation aggregation = new UnmappedSignificantTerms(name, requiredSize, minDocCount);
        return new NonCollectingAggregator(name, aggregationContext, parent) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    private static boolean hasParentBucketAggregator(Aggregator parent) {
        if (parent == null) {
            return false;
        } else if (parent.bucketAggregationMode() == BucketAggregationMode.PER_BUCKET) {
            return true;
        }
        return hasParentBucketAggregator(parent.parent());
    }

    @Override
    protected Aggregator create(ValuesSource valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
        
        numberOfAggregatorsCreated++;
        if (numberOfAggregatorsCreated == 1) {
            // Setup a termsEnum for use by first aggregator
            try {
                SearchContext searchContext = aggregationContext.searchContext();
                ContextIndexSearcher searcher = searchContext.searcher();
                Terms terms = MultiFields.getTerms(searcher.getIndexReader(), indexedFieldName);
                // terms can be null if the choice of field is not found in this index
                if (terms != null) {
                    termsEnum = terms.iterator(null);
                }
            } catch (IOException e) {
                throw new ElasticsearchException("IOException loading background document frequency info", e);
            }
        } else if (numberOfAggregatorsCreated == 2) {
            // When we have > 1 agg we have possibility of duplicate term frequency lookups and 
            // so introduce a cache in the form of a wrapper around the plain termsEnum created
            // for use with the first agg
            if (termsEnum != null) {
                SearchContext searchContext = aggregationContext.searchContext();
                termsEnum = new FrequencyCachingTermsEnumWrapper(termsEnum, searchContext.bigArrays(), true, false);
            }
        }
        
        long estimatedBucketCount = valuesSource.metaData().maxAtomicUniqueValuesCount();
        if (estimatedBucketCount < 0) {
            // there isn't an estimation available.. 50 should be a good start
            estimatedBucketCount = 50;
        }

        // adding an upper bound on the estimation as some atomic field data in the future (binary doc values) and not
        // going to know their exact cardinality and will return upper bounds in AtomicFieldData.getNumberUniqueValues()
        // that may be largely over-estimated.. the value chosen here is arbitrary just to play nice with typical CPU cache
        //
        // Another reason is that it may be faster to resize upon growth than to start directly with the appropriate size.
        // And that all values are not necessarily visited by the matches.
        estimatedBucketCount = Math.min(estimatedBucketCount, 512);

        if (valuesSource instanceof ValuesSource.Bytes) {
            if (executionHint != null && !executionHint.equals(EXECUTION_HINT_VALUE_MAP) && !executionHint.equals(EXECUTION_HINT_VALUE_ORDINALS)) {
                throw new ElasticsearchIllegalArgumentException("execution_hint can only be '" + EXECUTION_HINT_VALUE_MAP + "' or '" + EXECUTION_HINT_VALUE_ORDINALS + "', not " + executionHint);
            }
            String execution = executionHint;
            if (!(valuesSource instanceof ValuesSource.Bytes.WithOrdinals)) {
                execution = EXECUTION_HINT_VALUE_MAP;
            } else if (includeExclude != null) {
                execution = EXECUTION_HINT_VALUE_MAP;
            }
            if (execution == null) {
                if ((valuesSource instanceof ValuesSource.Bytes.WithOrdinals)
                        && !hasParentBucketAggregator(parent)) {
                    execution = EXECUTION_HINT_VALUE_ORDINALS;
                } else {
                    execution = EXECUTION_HINT_VALUE_MAP;
                }
            }
            assert execution != null;

            if (execution.equals(EXECUTION_HINT_VALUE_ORDINALS)) {
                assert includeExclude == null;
                return new SignificantStringTermsAggregator.WithOrdinals(name, factories, (ValuesSource.Bytes.WithOrdinals) valuesSource, estimatedBucketCount, requiredSize, shardSize, minDocCount, aggregationContext, parent, this);
            }
            return new SignificantStringTermsAggregator(name, factories, valuesSource, estimatedBucketCount, requiredSize, shardSize, minDocCount, includeExclude, aggregationContext, parent, this);
        }

        if (includeExclude != null) {
            throw new AggregationExecutionException("Aggregation [" + name + "] cannot support the include/exclude " +
                    "settings as it can only be applied to string values");
        }

        if (valuesSource instanceof ValuesSource.Numeric) {

            if (((ValuesSource.Numeric) valuesSource).isFloatingPoint()) {
                throw new UnsupportedOperationException("No support for examining floating point numerics");
            }
            return new SignificantLongTermsAggregator(name, factories, (ValuesSource.Numeric) valuesSource, formatter, estimatedBucketCount, requiredSize, shardSize, minDocCount, aggregationContext, parent, this);
        }

        throw new AggregationExecutionException("sigfnificant_terms aggregation cannot be applied to field [" + config.fieldContext().field() +
                "]. It can only be applied to numeric or string fields.");
    }

    public long getBackgroundFrequency(BytesRef termBytes) {
        assert termsEnum !=null; // having failed to find a field in the index we don't expect any calls for frequencies
        long result = 0;
        try {
            if (termsEnum.seekExact(termBytes)) {
                result = termsEnum.docFreq();
            }
        } catch (IOException e) {
            throw new ElasticsearchException("IOException loading background document frequency info", e);
        }
        return result;
    }


    public long getBackgroundFrequency(long term) {
        BytesRef indexedVal = mapper.indexedValueForSearch(term);
        return getBackgroundFrequency(indexedVal);
    }

    @Override
    public boolean release() throws ElasticsearchException {
        try {
            if (termsEnum instanceof Releasable) {
                ((Releasable) termsEnum).release();
            }
        } finally {
            termsEnum = null;
        }
        return true;
    }

    // A specialist TermsEnum wrapper for use in the repeated look-ups of frequency stats.
    // TODO factor out as a utility class to replace similar org.elasticsearch.search.suggest.phrase.WordScorer.FrequencyCachingTermsEnumWrapper
    // This implementation is likely to produce less garbage than WordScorer's impl but will need benchmarking/testing for that use case. 
    static class FrequencyCachingTermsEnumWrapper extends FilterTermsEnum implements Releasable {

        int currentTermDocFreq = 0;
        long currentTermTotalFreq = 0;
        private IntArray termDocFreqs;
        private LongArray termTotalFreqs;
        private BytesRefHash cachedTermOrds;
        protected BigArrays bigArrays;
        private boolean cacheDocFreqs;
        private boolean cacheTotalFreqs;
        private long currentTermOrd;

        public FrequencyCachingTermsEnumWrapper(TermsEnum delegate, BigArrays bigArrays, boolean cacheDocFreqs, boolean cacheTotalFreqs)  {
            super(delegate);
            this.bigArrays = bigArrays;
            this.cacheDocFreqs = cacheDocFreqs;
            this.cacheTotalFreqs = cacheTotalFreqs;
            if (cacheDocFreqs) {
                termDocFreqs = bigArrays.newIntArray(INITIAL_NUM_TERM_FREQS_CACHED, false);
            }
            if (cacheTotalFreqs) {
                termTotalFreqs = bigArrays.newLongArray(INITIAL_NUM_TERM_FREQS_CACHED, false);
            }
            cachedTermOrds = new BytesRefHash(INITIAL_NUM_TERM_FREQS_CACHED, bigArrays);
        }

        @Override
        public boolean seekExact(BytesRef text) throws IOException {
            currentTermDocFreq = 0;
            currentTermTotalFreq = 0;
            currentTermOrd = cachedTermOrds.add(text);
            if (currentTermOrd < 0) { // already seen, initialize instance data with the cached frequencies
                currentTermOrd = -1 - currentTermOrd;
                if (cacheDocFreqs) {
                    currentTermDocFreq = termDocFreqs.get(currentTermOrd);
                }
                if (cacheTotalFreqs) {
                    currentTermTotalFreq = termTotalFreqs.get(currentTermOrd);
                }
                return true;
            } else { // cache miss - pre-emptively read and cache the required frequency values
                if (in.seekExact(text)) {
                    if (cacheDocFreqs) {
                        currentTermDocFreq = in.docFreq();
                        termDocFreqs = bigArrays.grow(termDocFreqs, currentTermOrd + 1);
                        termDocFreqs.set(currentTermOrd, currentTermDocFreq);
                    }
                    if (cacheTotalFreqs) {
                        currentTermTotalFreq = in.totalTermFreq();
                        termTotalFreqs = bigArrays.grow(termTotalFreqs, currentTermOrd + 1);
                        termTotalFreqs.set(currentTermOrd, currentTermTotalFreq);
                    }
                    return true;
                }
            }
            return false;
        }

        @Override
        public long totalTermFreq() throws IOException {
            assert cacheTotalFreqs;
            return currentTermTotalFreq;
        }

        @Override
        public int docFreq() throws IOException {
            assert cacheDocFreqs;
            return currentTermDocFreq;
        }

        @Override
        public void seekExact(long ord) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
            throw new UnsupportedOperationException();
        }

        public SeekStatus seekCeil(BytesRef text) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytesRef next() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean release() throws ElasticsearchException {
            try {
                Releasables.release(cachedTermOrds, termDocFreqs, termTotalFreqs);
            } finally {
                cachedTermOrds = null;
                termDocFreqs = null;
                termTotalFreqs = null;
            }
            return true;
        }

    }
    
}
