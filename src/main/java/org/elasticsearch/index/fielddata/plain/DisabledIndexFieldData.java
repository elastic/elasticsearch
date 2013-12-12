/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMapper.Names;
import org.elasticsearch.index.settings.IndexSettings;

/**
 * A field data implementation that forbids loading and will throw an {@link ElasticSearchIllegalStateException} if you try to load
 * {@link AtomicFieldData} instances.
 */
public final class DisabledIndexFieldData extends AbstractIndexFieldData<AtomicFieldData<?>> {

    public static class Builder implements IndexFieldData.Builder {
        @Override
        public IndexFieldData<AtomicFieldData<?>> build(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType type, IndexFieldDataCache cache) {
            return new DisabledIndexFieldData(index, indexSettings, fieldNames, type, cache);
        }
    }

    public DisabledIndexFieldData(Index index, Settings indexSettings, Names fieldNames, FieldDataType fieldDataType, IndexFieldDataCache cache) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
    }

    @Override
    public boolean valuesOrdered() {
        return false;
    }

    @Override
    public AtomicFieldData<?> loadDirect(AtomicReaderContext context) throws Exception {
        throw fail();
    }

    @Override
    public IndexFieldData.XFieldComparatorSource comparatorSource(Object missingValue, SortMode sortMode) {
        throw fail();
    }

    private ElasticSearchIllegalStateException fail() {
        return new ElasticSearchIllegalStateException("Field data loading is forbidden on " + getFieldNames().name());
    }

}
