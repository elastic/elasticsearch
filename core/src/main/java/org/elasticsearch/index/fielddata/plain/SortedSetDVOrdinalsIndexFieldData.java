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
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.AtomicOrdinalsFieldData;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsBuilder;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

import java.io.IOException;

public class SortedSetDVOrdinalsIndexFieldData extends DocValuesIndexFieldData implements IndexOrdinalsFieldData {

    private final IndexSettings indexSettings;
    private final IndexFieldDataCache cache;
    private final CircuitBreakerService breakerService;

    public SortedSetDVOrdinalsIndexFieldData(IndexSettings indexSettings, IndexFieldDataCache cache, String fieldName, CircuitBreakerService breakerService, FieldDataType fieldDataType) {
        super(indexSettings.getIndex(), fieldName, fieldDataType);
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
        return new SortedSetDVBytesAtomicFieldData(context.reader(), fieldName);
    }

    @Override
    public AtomicOrdinalsFieldData loadDirect(LeafReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader) {
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
        } catch (Throwable e) {
            if (e instanceof ElasticsearchException) {
                throw (ElasticsearchException) e;
            } else {
                throw new ElasticsearchException(e);
            }
        }
    }

    @Override
    public IndexOrdinalsFieldData localGlobalDirect(DirectoryReader indexReader) throws Exception {
        return GlobalOrdinalsBuilder.build(indexReader, this, indexSettings, breakerService, logger);
    }
}
