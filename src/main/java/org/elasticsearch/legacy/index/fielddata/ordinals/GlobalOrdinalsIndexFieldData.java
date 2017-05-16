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
package org.elasticsearch.legacy.index.fielddata.ordinals;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.legacy.common.Nullable;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.index.AbstractIndexComponent;
import org.elasticsearch.legacy.index.Index;
import org.elasticsearch.legacy.index.fielddata.AtomicFieldData;
import org.elasticsearch.legacy.index.fielddata.FieldDataType;
import org.elasticsearch.legacy.index.fielddata.IndexFieldData;
import org.elasticsearch.legacy.index.mapper.FieldMapper;
import org.elasticsearch.legacy.search.MultiValueMode;

/**
 * {@link IndexFieldData} base class for concrete global ordinals implementations.
 */
public abstract class GlobalOrdinalsIndexFieldData extends AbstractIndexComponent implements IndexFieldData.WithOrdinals, Accountable {

    private final FieldMapper.Names fieldNames;
    private final FieldDataType fieldDataType;
    private final long memorySizeInBytes;

    protected GlobalOrdinalsIndexFieldData(Index index, Settings settings, FieldMapper.Names fieldNames, FieldDataType fieldDataType, long memorySizeInBytes) {
        super(index, settings);
        this.fieldNames = fieldNames;
        this.fieldDataType = fieldDataType;
        this.memorySizeInBytes = memorySizeInBytes;
    }

    @Override
    public AtomicFieldData.WithOrdinals loadDirect(AtomicReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public WithOrdinals loadGlobal(IndexReader indexReader) {
        return this;
    }

    @Override
    public WithOrdinals localGlobalDirect(IndexReader indexReader) throws Exception {
        return this;
    }

    @Override
    public FieldMapper.Names getFieldNames() {
        return fieldNames;
    }

    @Override
    public FieldDataType getFieldDataType() {
        return fieldDataType;
    }

    @Override
    public boolean valuesOrdered() {
        return false;
    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, MultiValueMode sortMode) {
        throw new UnsupportedOperationException("no global ordinals sorting yet");
    }

    @Override
    public void clear() {
        // no need to clear, because this is cached and cleared in AbstractBytesIndexFieldData
    }

    @Override
    public void clear(IndexReader reader) {
        // no need to clear, because this is cached and cleared in AbstractBytesIndexFieldData
    }

    @Override
    public long ramBytesUsed() {
        return memorySizeInBytes;
    }

}
