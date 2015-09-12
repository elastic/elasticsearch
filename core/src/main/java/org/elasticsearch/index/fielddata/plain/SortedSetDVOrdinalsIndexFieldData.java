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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsBuilder;
import org.elasticsearch.index.mapper.MappedFieldType.Names;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

public class SortedSetDVOrdinalsIndexFieldData extends DocValuesIndexFieldData implements IndexOrdinalsFieldData {

    private final Settings indexSettings;
    private final IndexFieldDataCache cache;
    private final CircuitBreakerService breakerService;

    public SortedSetDVOrdinalsIndexFieldData(Index index, IndexFieldDataCache cache, Settings indexSettings, Names fieldNames, CircuitBreakerService breakerService, FieldDataType fieldDataType) {
        super(index, fieldNames, fieldDataType);
        this.indexSettings = indexSettings;
        this.cache = cache;
        this.breakerService = breakerService;
    }

    @Override
    public org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource comparatorSource(Object missingValue, MultiValueMode sortMode, Nested nested) {
        return new BytesRefFieldComparatorSource((IndexFieldData<?>) this, missingValue, sortMode, nested);
    }

    @Override
    public AtomicOrdinalsFieldData load(LeafReaderContext context) {
        return new SortedSetDVBytesAtomicFieldData(context.reader(), fieldNames.indexName());
    }

    @Override
    public AtomicOrdinalsFieldData loadDirect(LeafReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public IndexOrdinalsFieldData loadGlobal(IndexReader indexReader) {
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
    public IndexOrdinalsFieldData localGlobalDirect(IndexReader indexReader) throws Exception {
        return GlobalOrdinalsBuilder.build(indexReader, this, indexSettings, breakerService, logger);
    }
}
