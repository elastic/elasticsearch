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
package org.elasticsearch.index.mapper.ip;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.search.MultiValueMode;

final class LegacyIpIndexFieldData implements IndexFieldData<AtomicFieldData> {

    protected final Index index;
    protected final String fieldName;
    protected final ESLogger logger;

    public LegacyIpIndexFieldData(Index index, String fieldName) {
        this.index = index;
        this.fieldName = fieldName;
        this.logger = Loggers.getLogger(getClass());
    }

    public final String getFieldName() {
        return fieldName;
    }

    public final void clear() {
        // nothing to do
    }

    public final void clear(IndexReader reader) {
        // nothing to do
    }

    public final Index index() {
        return index;
    }

    @Override
    public AtomicFieldData load(LeafReaderContext context) {
        return new AtomicFieldData() {
            
            @Override
            public void close() {
                // no-op
            }
            
            @Override
            public long ramBytesUsed() {
                return 0;
            }
            
            @Override
            public ScriptDocValues<?> getScriptValues() {
                throw new UnsupportedOperationException("Cannot run scripts on ip fields");
            }
            
            @Override
            public SortedBinaryDocValues getBytesValues() {
                SortedNumericDocValues values;
                try {
                    values = DocValues.getSortedNumeric(context.reader(), fieldName);
                } catch (IOException e) {
                    throw new IllegalStateException("Cannot load doc values", e);
                }
                return new SortedBinaryDocValues() {

                    final ByteBuffer scratch = ByteBuffer.allocate(4);

                    @Override
                    public BytesRef valueAt(int index) {
                        // we do not need to reorder ip addresses since both the numeric
                        // encoding of LegacyIpFieldMapper and the binary encoding of
                        // IpFieldMapper match the sort order of ip addresses
                        long ip = values.valueAt(index);
                        scratch.putInt(0, (int) ip);
                        InetAddress inet;
                        try {
                            inet = InetAddress.getByAddress(scratch.array());
                        } catch (UnknownHostException e) {
                            throw new IllegalStateException("Cannot happen", e);
                        }
                        byte[] encoded = InetAddressPoint.encode(inet);
                        return new BytesRef(encoded);
                    }
                    
                    @Override
                    public void setDocument(int docId) {
                        values.setDocument(docId);
                    }
                    
                    @Override
                    public int count() {
                        return values.count();
                    }
                };
            }
        };
    }

    @Override
    public AtomicFieldData loadDirect(LeafReaderContext context)
            throws Exception {
        return load(context);
    }

    @Override
    public IndexFieldData.XFieldComparatorSource comparatorSource(
            Object missingValue, MultiValueMode sortMode, Nested nested) {
        return new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
    }

}
