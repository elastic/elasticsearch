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
package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.AtomicOrdinalsFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.search.MultiValueMode;

import java.util.Collection;
import java.util.Collections;

/**
 * {@link IndexFieldData} base class for concrete global ordinals implementations.
 */
public abstract class GlobalOrdinalsIndexFieldData extends AbstractIndexComponent implements IndexOrdinalsFieldData, Accountable {

    private final String fieldName;
    private final long memorySizeInBytes;

    protected GlobalOrdinalsIndexFieldData(IndexSettings indexSettings, String fieldName, long memorySizeInBytes) {
        super(indexSettings);
        this.fieldName = fieldName;
        this.memorySizeInBytes = memorySizeInBytes;
    }

    @Override
    public AtomicOrdinalsFieldData loadDirect(LeafReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader) {
        return this;
    }

    @Override
    public IndexOrdinalsFieldData localGlobalDirect(DirectoryReader indexReader) throws Exception {
        return this;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested) {
        throw new UnsupportedOperationException("no global ordinals sorting yet");
    }

    @Override
    public void clear() {
        // no need to clear, because this is cached and cleared in AbstractBytesIndexFieldData
    }

    @Override
    public long ramBytesUsed() {
        return memorySizeInBytes;
    }

    @Override
    public Collection<Accountable> getChildResources() {
        // TODO: break down ram usage?
        return Collections.emptyList();
    }
}
