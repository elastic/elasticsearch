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
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsBuilder;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;
import org.elasticsearch.search.MultiValueMode;

public class IndexIndexFieldData implements IndexFieldData.WithOrdinals<AtomicFieldData.WithOrdinals<ScriptDocValues>> {

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData<?> build(Index index, Settings indexSettings, FieldMapper<?> mapper, IndexFieldDataCache cache,
                CircuitBreakerService breakerService, MapperService mapperService, GlobalOrdinalsBuilder globalOrdinalBuilder) {
            return new IndexIndexFieldData(index, mapper.names());
        }

    }

    private static final Ordinals.Docs INDEX_ORDINALS = new Ordinals.Docs() {

        @Override
        public int setDocument(int docId) {
            return 1;
        }

        @Override
        public long nextOrd() {
            return Ordinals.MIN_ORDINAL;
        }

        @Override
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public long getOrd(int docId) {
            return Ordinals.MIN_ORDINAL;
        }

        @Override
        public long getMaxOrd() {
            return 1;
        }

        @Override
        public long currentOrd() {
            return Ordinals.MIN_ORDINAL;
        }
    };

    private static class IndexBytesValues extends BytesValues.WithOrdinals {

        final int hash;

        protected IndexBytesValues(String index) {
            super(INDEX_ORDINALS);
            scratch.copyChars(index);
            hash = scratch.hashCode();
        }

        @Override
        public int currentValueHash() {
            return hash;
        }

        @Override
        public BytesRef getValueByOrd(long ord) {
            return scratch;
        }

    }

    private static class IndexAtomicFieldData implements AtomicFieldData.WithOrdinals<ScriptDocValues> {

        private final String index;

        IndexAtomicFieldData(String index) {
            this.index = index;
        }

        @Override
        public long getMemorySizeInBytes() {
            return 0;
        }

        @Override
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public long getNumberUniqueValues() {
            return 1;
        }

        @Override
        public BytesValues.WithOrdinals getBytesValues(boolean needsHashes) {
            return new IndexBytesValues(index);
        }

        @Override
        public ScriptDocValues getScriptValues() {
            return new ScriptDocValues.Strings(getBytesValues(false));
        }

        @Override
        public void close() {
        }

        @Override
        public TermsEnum getTermsEnum() {
            return new AtomicFieldDataWithOrdinalsTermsEnum(this);
        }

    }

    private final FieldMapper.Names names;
    private final Index index;

    private IndexIndexFieldData(Index index, FieldMapper.Names names) {
        this.index = index;
        this.names = names;
    }

    @Override
    public Index index() {
        return index;
    }

    @Override
    public org.elasticsearch.index.mapper.FieldMapper.Names getFieldNames() {
        return names;
    }

    @Override
    public FieldDataType getFieldDataType() {
        return new FieldDataType("string");
    }

    @Override
    public boolean valuesOrdered() {
        return true;
    }

    @Override
    public org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource comparatorSource(Object missingValue,
            MultiValueMode sortMode) {
        return new BytesRefFieldComparatorSource(this, missingValue, sortMode);
    }

    @Override
    public void clear() {
    }

    @Override
    public void clear(IndexReader reader) {
    }

    @Override
    public AtomicFieldData.WithOrdinals<ScriptDocValues> load(AtomicReaderContext context) {
        return new IndexAtomicFieldData(index().name());
    }

    @Override
    public AtomicFieldData.WithOrdinals<ScriptDocValues> loadDirect(AtomicReaderContext context)
            throws Exception {
        return load(context);
    }

    @Override
    public IndexFieldData.WithOrdinals<?> loadGlobal(IndexReader indexReader) {
        return this;
    }

    @Override
    public IndexFieldData.WithOrdinals<?> localGlobalDirect(IndexReader indexReader) throws Exception {
        return loadGlobal(indexReader);
    }

}
