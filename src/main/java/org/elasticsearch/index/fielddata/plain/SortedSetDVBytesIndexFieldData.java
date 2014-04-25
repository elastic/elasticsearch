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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper.Names;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;

public class SortedSetDVBytesIndexFieldData extends DocValuesIndexFieldData implements IndexFieldData.WithOrdinals<SortedSetDVBytesAtomicFieldData> {

    private final Settings indexSettings;
    private final IndexFieldDataCache cache;
    private final GlobalOrdinalsBuilder globalOrdinalsBuilder;
    private final CircuitBreakerService breakerService;

    public SortedSetDVBytesIndexFieldData(Index index, IndexFieldDataCache cache, Settings indexSettings, Names fieldNames, GlobalOrdinalsBuilder globalOrdinalBuilder, CircuitBreakerService breakerService, FieldDataType fieldDataType) {
        super(index, fieldNames, fieldDataType);
        this.indexSettings = indexSettings;
        this.cache = cache;
        this.globalOrdinalsBuilder = globalOrdinalBuilder;
        this.breakerService = breakerService;
    }

    @Override
    public boolean valuesOrdered() {
        return true;
    }

    public org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource comparatorSource(Object missingValue, MultiValueMode sortMode) {
        return new BytesRefFieldComparatorSource((IndexFieldData<?>) this, missingValue, sortMode);
    }

    @Override
    public SortedSetDVBytesAtomicFieldData load(AtomicReaderContext context) {
        return new SortedSetDVBytesAtomicFieldData(context.reader(), fieldNames.indexName());
    }

    @Override
    public SortedSetDVBytesAtomicFieldData loadDirect(AtomicReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public WithOrdinals loadGlobal(IndexReader indexReader) {
        if (indexReader.leaves().size() <= 1) {
            // ordinals are already global
            return this;
        }
        try {
            return cache.load(indexReader, this);
        } catch (Throwable e) {
            if (e instanceof ElasticsearchException) {
                throw (ElasticsearchException) e;
            } else {
                throw new ElasticsearchException(e.getMessage(), e);
            }
        }
    }

    @Override
    public WithOrdinals localGlobalDirect(IndexReader indexReader) throws Exception {
        return globalOrdinalsBuilder.build(indexReader, this, indexSettings, breakerService);
    }
}
