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

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;

final class BytesBinaryDVAtomicFieldData implements AtomicFieldData<ScriptDocValues> {

    private final AtomicReader reader;
    private final BinaryDocValues values;

    BytesBinaryDVAtomicFieldData(AtomicReader reader, BinaryDocValues values) {
        super();
        this.reader = reader;
        this.values = values == null ? DocValues.EMPTY_BINARY : values;
    }

    @Override
    public boolean isMultiValued() {
        return true;
    }

    @Override
    public long getNumberUniqueValues() {
        return Long.MAX_VALUE;
    }

    @Override
    public long getMemorySizeInBytes() {
        return -1; // not exposed by Lucene
    }

    @Override
    public BytesValues getBytesValues(boolean needsHashes) {
        return new BytesValues(true) {

            final BytesRef bytes = new BytesRef();
            final ByteArrayDataInput in = new ByteArrayDataInput();

            @Override
            public int setDocument(int docId) {
                values.get(docId, bytes);
                in.reset(bytes.bytes, bytes.offset, bytes.length);
                if (bytes.length == 0) {
                    return 0;
                } else {
                    return in.readVInt();
                }
            }

            @Override
            public BytesRef nextValue() {
                final int length = in.readVInt();
                scratch.grow(length);
                in.readBytes(scratch.bytes, 0, length);
                scratch.length = length;
                scratch.offset = 0;
                return scratch;
            }

        };
    }

    @Override
    public ScriptDocValues getScriptValues() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        // no-op
    }


}
