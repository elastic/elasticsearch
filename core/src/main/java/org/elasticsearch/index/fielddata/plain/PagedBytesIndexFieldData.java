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
package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.codecs.blocktree.FieldReader;
import org.apache.lucene.codecs.blocktree.Stats;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.AtomicOrdinalsFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.RamAccountingTermsEnum;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

import java.io.IOException;

/**
 */
public class PagedBytesIndexFieldData extends AbstractIndexOrdinalsFieldData {


    public static class Builder implements IndexFieldData.Builder {

        private final double minFrequency, maxFrequency;
        private final int minSegmentSize;

        public Builder(double minFrequency, double maxFrequency, int minSegmentSize) {
            this.minFrequency = minFrequency;
            this.maxFrequency = maxFrequency;
            this.minSegmentSize = minSegmentSize;
        }

        @Override
        public IndexOrdinalsFieldData build(IndexSettings indexSettings, MappedFieldType fieldType,
                                                               IndexFieldDataCache cache, CircuitBreakerService breakerService, MapperService mapperService) {
            return new PagedBytesIndexFieldData(indexSettings, fieldType.name(), cache, breakerService,
                    minFrequency, maxFrequency, minSegmentSize);
        }
    }

    public PagedBytesIndexFieldData(IndexSettings indexSettings, String fieldName,
                                    IndexFieldDataCache cache, CircuitBreakerService breakerService,
                                    double minFrequency, double maxFrequency, int minSegmentSize) {
        super(indexSettings, fieldName, cache, breakerService, minFrequency, maxFrequency, minSegmentSize);
    }

    @Override
    public AtomicOrdinalsFieldData loadDirect(LeafReaderContext context) throws Exception {
        LeafReader reader = context.reader();
        AtomicOrdinalsFieldData data = null;

        PagedBytesEstimator estimator = new PagedBytesEstimator(context, breakerService.getBreaker(CircuitBreaker.FIELDDATA), getFieldName());
        Terms terms = reader.terms(getFieldName());
        if (terms == null) {
            data = AbstractAtomicOrdinalsFieldData.empty();
            estimator.afterLoad(null, data.ramBytesUsed());
            return data;
        }

        final PagedBytes bytes = new PagedBytes(15);

        final PackedLongValues.Builder termOrdToBytesOffset = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        final float acceptableTransientOverheadRatio = OrdinalsBuilder.DEFAULT_ACCEPTABLE_OVERHEAD_RATIO;

        // Wrap the context in an estimator and use it to either estimate
        // the entire set, or wrap the TermsEnum so it can be calculated
        // per-term

        TermsEnum termsEnum = estimator.beforeLoad(terms);
        boolean success = false;

        try (OrdinalsBuilder builder = new OrdinalsBuilder(reader.maxDoc(), acceptableTransientOverheadRatio)) {
            PostingsEnum docsEnum = null;
            for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
                final long termOrd = builder.nextOrdinal();
                assert termOrd == termOrdToBytesOffset.size();
                termOrdToBytesOffset.add(bytes.copyUsingLengthPrefix(term));
                docsEnum = termsEnum.postings(docsEnum, PostingsEnum.NONE);
                for (int docId = docsEnum.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                    builder.addDoc(docId);
                }
            }
            PagedBytes.Reader bytesReader = bytes.freeze(true);
            final Ordinals ordinals = builder.build();

            data = new PagedBytesAtomicFieldData(bytesReader, termOrdToBytesOffset.build(), ordinals);
            success = true;
            return data;
        } finally {
            if (!success) {
                // If something went wrong, unwind any current estimations we've made
                estimator.afterLoad(termsEnum, 0);
            } else {
                // Call .afterLoad() to adjust the breaker now that we have an exact size
                estimator.afterLoad(termsEnum, data.ramBytesUsed());
            }

        }
    }

    /**
     * Estimator that wraps string field data by either using
     * BlockTreeTermsReader, or wrapping the data in a RamAccountingTermsEnum
     * if the BlockTreeTermsReader cannot be used.
     */
    public class PagedBytesEstimator implements PerValueEstimator {

        private final LeafReaderContext context;
        private final CircuitBreaker breaker;
        private final String fieldName;
        private long estimatedBytes;

        PagedBytesEstimator(LeafReaderContext context, CircuitBreaker breaker, String fieldName) {
            this.breaker = breaker;
            this.context = context;
            this.fieldName = fieldName;
        }

        /**
         * @return the number of bytes for the term based on the length and ordinal overhead
         */
        @Override
        public long bytesPerValue(BytesRef term) {
            if (term == null) {
                return 0;
            }
            long bytes = term.length;
            // 64 bytes for miscellaneous overhead
            bytes += 64;
            // Seems to be about a 1.5x compression per term/ord, plus 1 for some wiggle room
            bytes = (long) ((double) bytes / 1.5) + 1;
            return bytes;
        }

        /**
         * @return the estimate for loading the entire term set into field data, or 0 if unavailable
         */
        public long estimateStringFieldData() {
            try {
                LeafReader reader = context.reader();
                Terms terms = reader.terms(getFieldName());

                Fields fields = reader.fields();
                final Terms fieldTerms = fields.terms(getFieldName());

                if (fieldTerms instanceof FieldReader) {
                    final Stats stats = ((FieldReader) fieldTerms).getStats();
                    long totalTermBytes = stats.totalTermBytes;
                    if (logger.isTraceEnabled()) {
                        logger.trace("totalTermBytes: {}, terms.size(): {}, terms.getSumDocFreq(): {}",
                                totalTermBytes, terms.size(), terms.getSumDocFreq());
                    }
                    long totalBytes = totalTermBytes + (2 * terms.size()) + (4 * terms.getSumDocFreq());
                    return totalBytes;
                }
            } catch (Exception e) {
                logger.warn("Unable to estimate memory overhead", e);
            }
            return 0;
        }

        /**
         * Determine whether the BlockTreeTermsReader.FieldReader can be used
         * for estimating the field data, adding the estimate to the circuit
         * breaker if it can, otherwise wrapping the terms in a
         * RamAccountingTermsEnum to be estimated on a per-term basis.
         *
         * @param terms terms to be estimated
         * @return A possibly wrapped TermsEnum for the terms
         */
        @Override
        public TermsEnum beforeLoad(Terms terms) throws IOException {
            LeafReader reader = context.reader();

            TermsEnum iterator = terms.iterator();
            TermsEnum filteredIterator = filter(terms, iterator, reader);
            final boolean filtered = iterator != filteredIterator;
            iterator = filteredIterator;

            if (filtered) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Filter exists, can't circuit break normally, using RamAccountingTermsEnum");
                }
                return new RamAccountingTermsEnum(iterator, breaker, this, this.fieldName);
            } else {
                estimatedBytes = this.estimateStringFieldData();
                // If we weren't able to estimate, wrap in the RamAccountingTermsEnum
                if (estimatedBytes == 0) {
                    iterator = new RamAccountingTermsEnum(iterator, breaker, this, this.fieldName);
                } else {
                    breaker.addEstimateBytesAndMaybeBreak(estimatedBytes, fieldName);
                }

                return iterator;
            }
        }

        /**
         * Adjust the circuit breaker now that terms have been loaded, getting
         * the actual used either from the parameter (if estimation worked for
         * the entire set), or from the TermsEnum if it has been wrapped in a
         * RamAccountingTermsEnum.
         *
         * @param termsEnum  terms that were loaded
         * @param actualUsed actual field data memory usage
         */
        @Override
        public void afterLoad(TermsEnum termsEnum, long actualUsed) {
            if (termsEnum instanceof RamAccountingTermsEnum) {
                estimatedBytes = ((RamAccountingTermsEnum) termsEnum).getTotalBytes();
            }
            breaker.addWithoutBreaking(-(estimatedBytes - actualUsed));
        }

        /**
         * Adjust the breaker when no terms were actually loaded, but the field
         * data takes up space regardless. For instance, when ordinals are
         * used.
         * @param actualUsed bytes actually used
         */
        public void adjustForNoTerms(long actualUsed) {
            breaker.addWithoutBreaking(actualUsed);
        }
    }
}
