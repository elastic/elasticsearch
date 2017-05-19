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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.plain.AbstractAtomicOrdinalsFieldData;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.search.MultiValueMode;

/** Fielddata view of the _uid field on indices that do not index _uid but _id.
 *  It gets fielddata on the {@code _id field}, which is in-memory since the _id
 *  field does not have doc values, and prepends {@code ${type}#} to all values.
 *  Note that it does not add memory compared to what fielddata on the _id is
 *  already using: this is just a view.
 *  TODO: Remove fielddata access on _uid and _id, or add doc values to _id.
 */
public final class UidIndexFieldData implements IndexOrdinalsFieldData {

    private final Index index;
    private final String type;
    private final BytesRef prefix;
    private final IndexOrdinalsFieldData idFieldData;

    public UidIndexFieldData(Index index, String type, IndexOrdinalsFieldData idFieldData) {
        this.index = index;
        this.type = type;
        BytesRefBuilder prefix = new BytesRefBuilder();
        prefix.append(new BytesRef(type));
        prefix.append((byte) '#');
        this.prefix = prefix.toBytesRef();
        this.idFieldData = idFieldData;
    }

    @Override
    public Index index() {
        return index;
    }

    @Override
    public String getFieldName() {
        return UidFieldMapper.NAME;
    }

    @Override
    public SortField sortField(Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
        XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
        return new SortField(getFieldName(), source, reverse);
    }

    @Override
    public AtomicOrdinalsFieldData load(LeafReaderContext context) {
        return new UidAtomicFieldData(prefix, idFieldData.load(context));
    }

    @Override
    public AtomicOrdinalsFieldData loadDirect(LeafReaderContext context) throws Exception {
        return new UidAtomicFieldData(prefix, idFieldData.loadDirect(context));
    }

    @Override
    public void clear() {
        idFieldData.clear();
    }

    @Override
    public IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader) {
        return new UidIndexFieldData(index, type, idFieldData.loadGlobal(indexReader));
    }

    @Override
    public IndexOrdinalsFieldData localGlobalDirect(DirectoryReader indexReader) throws Exception {
        return new UidIndexFieldData(index, type, idFieldData.localGlobalDirect(indexReader));
    }

    @Override
    public MultiDocValues.OrdinalMap getOrdinalMap() {
        return idFieldData.getOrdinalMap();
    }

    static final class UidAtomicFieldData implements AtomicOrdinalsFieldData {

        private final BytesRef prefix;
        private final AtomicOrdinalsFieldData idFieldData;

        UidAtomicFieldData(BytesRef prefix, AtomicOrdinalsFieldData idFieldData) {
            this.prefix = prefix;
            this.idFieldData = idFieldData;
        }

        @Override
        public ScriptDocValues<?> getScriptValues() {
            return AbstractAtomicOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION.apply(getOrdinalsValues());
        }

        @Override
        public SortedBinaryDocValues getBytesValues() {
            return FieldData.toString(getOrdinalsValues());
        }

        @Override
        public long ramBytesUsed() {
            return 0; // simple wrapper
        }

        @Override
        public void close() {
            idFieldData.close();
        }

        @Override
        public RandomAccessOrds getOrdinalsValues() {
            RandomAccessOrds idValues = idFieldData.getOrdinalsValues();
            return new AbstractRandomAccessOrds() {

                private final BytesRefBuilder scratch = new BytesRefBuilder();

                @Override
                public BytesRef lookupOrd(long ord) {
                    scratch.setLength(0);
                    scratch.append(prefix);
                    scratch.append(idValues.lookupOrd(ord));
                    return scratch.get();
                }

                @Override
                public long getValueCount() {
                    return idValues.getValueCount();
                }

                @Override
                protected void doSetDocument(int docID) {
                    idValues.setDocument(docID);
                }

                @Override
                public long ordAt(int index) {
                    return idValues.ordAt(index);
                }

                @Override
                public int cardinality() {
                    return idValues.cardinality();
                }
            };
        }

    }

}
