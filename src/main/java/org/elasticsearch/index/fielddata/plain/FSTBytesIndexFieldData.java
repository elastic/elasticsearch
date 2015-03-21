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

import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.INPUT_TYPE;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

/**
 */
public class FSTBytesIndexFieldData extends AbstractIndexOrdinalsFieldData {

    private final CircuitBreakerService breakerService;

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexOrdinalsFieldData build(Index index, @IndexSettings Settings indexSettings, FieldMapper<?> mapper,
                                                             IndexFieldDataCache cache, CircuitBreakerService breakerService, MapperService mapperService) {
            return new FSTBytesIndexFieldData(index, indexSettings, mapper.names(), mapper.fieldDataType(), cache, breakerService);
        }
    }

    FSTBytesIndexFieldData(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType fieldDataType,
                           IndexFieldDataCache cache, CircuitBreakerService breakerService) {
        super(index, indexSettings, fieldNames, fieldDataType, cache, breakerService);
        this.breakerService = breakerService;
    }

    @Override
    public AtomicOrdinalsFieldData loadDirect(LeafReaderContext context) throws Exception {
        LeafReader reader = context.reader();

        Terms terms = reader.terms(getFieldNames().indexName());
        AtomicOrdinalsFieldData data = null;
        // TODO: Use an actual estimator to estimate before loading.
        NonEstimatingEstimator estimator = new NonEstimatingEstimator(breakerService.getBreaker(CircuitBreaker.FIELDDATA));
        if (terms == null) {
            data = AbstractAtomicOrdinalsFieldData.empty();
            estimator.afterLoad(null, data.ramBytesUsed());
            return data;
        }
        PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
        org.apache.lucene.util.fst.Builder<Long> fstBuilder = new org.apache.lucene.util.fst.Builder<>(INPUT_TYPE.BYTE1, outputs);
        final IntsRefBuilder scratch = new IntsRefBuilder();

        final long numTerms;
        if (regex == null && frequency == null) {
            numTerms = terms.size();
        } else {
            numTerms = -1;
        }
        final float acceptableTransientOverheadRatio = fieldDataType.getSettings().getAsFloat("acceptable_transient_overhead_ratio", OrdinalsBuilder.DEFAULT_ACCEPTABLE_OVERHEAD_RATIO);
        boolean success = false;
        try (OrdinalsBuilder builder = new OrdinalsBuilder(numTerms, reader.maxDoc(), acceptableTransientOverheadRatio)) {

            // we don't store an ord 0 in the FST since we could have an empty string in there and FST don't support
            // empty strings twice. ie. them merge fails for long output.
            TermsEnum termsEnum = filter(terms, reader);
            PostingsEnum docsEnum = null;
            for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
                final long termOrd = builder.nextOrdinal();
                fstBuilder.add(Util.toIntsRef(term, scratch), (long) termOrd);
                docsEnum = termsEnum.postings(null, docsEnum, PostingsEnum.NONE);
                for (int docId = docsEnum.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                    builder.addDoc(docId);
                }
            }

            FST<Long> fst = fstBuilder.finish();

            final Ordinals ordinals = builder.build(fieldDataType.getSettings());

            data = new FSTBytesAtomicFieldData(fst, ordinals);
            success = true;
            return data;
        } finally {
            if (success) {
                estimator.afterLoad(null, data.ramBytesUsed());
            }

        }
    }
}
