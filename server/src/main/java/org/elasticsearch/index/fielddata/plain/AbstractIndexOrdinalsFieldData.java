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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.AtomicOrdinalsFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsBuilder;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsIndexFieldData;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

import java.io.IOException;

public abstract class AbstractIndexOrdinalsFieldData extends AbstractIndexFieldData<AtomicOrdinalsFieldData>
        implements IndexOrdinalsFieldData {

    private final double minFrequency, maxFrequency;
    private final int minSegmentSize;
    protected final CircuitBreakerService breakerService;

    protected AbstractIndexOrdinalsFieldData(IndexSettings indexSettings, String fieldName,
            IndexFieldDataCache cache, CircuitBreakerService breakerService,
            double minFrequency, double maxFrequency, int minSegmentSize) {
        super(indexSettings, fieldName, cache);
        this.breakerService = breakerService;
        this.minFrequency = minFrequency;
        this.maxFrequency = maxFrequency;
        this.minSegmentSize = minSegmentSize;
    }

    @Override
    public OrdinalMap getOrdinalMap() {
        return null;
    }

    @Override
    public IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader) {
        IndexOrdinalsFieldData fieldData = loadGlobalInternal(indexReader);
        if (fieldData instanceof GlobalOrdinalsIndexFieldData) {
            // we create a new instance of the cached value for each consumer in order
            // to avoid creating new TermsEnums for each segment in the cached instance
            return ((GlobalOrdinalsIndexFieldData) fieldData).newConsumer(indexReader);
        } else {
            return fieldData;
        }
    }

    private IndexOrdinalsFieldData loadGlobalInternal(DirectoryReader indexReader) {
        if (indexReader.leaves().size() <= 1) {
            // ordinals are already global
            return this;
        }
        boolean fieldFound = false;
        for (LeafReaderContext context : indexReader.leaves()) {
            if (context.reader().getFieldInfos().fieldInfo(getFieldName()) != null) {
                fieldFound = true;
                break;
            }
        }
        if (fieldFound == false) {
            // Some directory readers may be wrapped and report different set of fields and use the same cache key.
            // If a field can't be found then it doesn't mean it isn't there,
            // so if a field doesn't exist then we don't cache it and just return an empty field data instance.
            // The next time the field is found, we do cache.
            try {
                return GlobalOrdinalsBuilder.buildEmpty(indexSettings, indexReader, this);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            return cache.load(indexReader, this);
        } catch (Exception e) {
            if (e instanceof ElasticsearchException) {
                throw (ElasticsearchException) e;
            } else {
                throw new ElasticsearchException(e);
            }
        }
    }

    @Override
    public IndexOrdinalsFieldData localGlobalDirect(DirectoryReader indexReader) throws Exception {
        return GlobalOrdinalsBuilder.build(indexReader, this, indexSettings, breakerService, logger,
                AbstractAtomicOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION);
    }

    @Override
    protected AtomicOrdinalsFieldData empty(int maxDoc) {
        return AbstractAtomicOrdinalsFieldData.empty();
    }

    protected TermsEnum filter(Terms terms, TermsEnum iterator, LeafReader reader) throws IOException {
        if (iterator == null) {
            return null;
        }
        int docCount = terms.getDocCount();
        if (docCount == -1) {
            docCount = reader.maxDoc();
        }
        if (docCount >= minSegmentSize) {
            final int minFreq = minFrequency > 1.0
                    ? (int) minFrequency
                    : (int)(docCount * minFrequency);
            final int maxFreq = maxFrequency > 1.0
                    ? (int) maxFrequency
                    : (int)(docCount * maxFrequency);
            if (minFreq > 1 || maxFreq < docCount) {
                iterator = new FrequencyFilter(iterator, minFreq, maxFreq);
            }
        }
        return iterator;
    }

    @Override
    public boolean supportsGlobalOrdinalsMapping() {
        return false;
    }

    private static final class FrequencyFilter extends FilteredTermsEnum {

        private int minFreq;
        private int maxFreq;
        FrequencyFilter(TermsEnum delegate, int minFreq, int maxFreq) {
            super(delegate, false);
            this.minFreq = minFreq;
            this.maxFreq = maxFreq;
        }

        @Override
        protected AcceptStatus accept(BytesRef arg0) throws IOException {
            int docFreq = docFreq();
            if (docFreq >= minFreq && docFreq <= maxFreq) {
                return AcceptStatus.YES;
            }
            return AcceptStatus.NO;
        }
    }

}
