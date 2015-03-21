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

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

final class BytesBinaryDVAtomicFieldData implements AtomicFieldData {

    private final BinaryDocValues values;

    BytesBinaryDVAtomicFieldData(BinaryDocValues values) {
        super();
        this.values = values;
    }

    @Override
    public long ramBytesUsed() {
        return 0; // not exposed by Lucene
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

    @Override
    public SortedBinaryDocValues getBytesValues() {
        return new SortedBinaryDocValues() {

            int count;
            BytesRefBuilder[] refs = new BytesRefBuilder[0];
            final ByteArrayDataInput in = new ByteArrayDataInput();

            @Override
            public void setDocument(int docId) {
                final BytesRef bytes = values.get(docId);
                in.reset(bytes.bytes, bytes.offset, bytes.length);
                if (bytes.length == 0) {
                    count = 0;
                } else {
                    count = in.readVInt();
                    if (count > refs.length) {
                        final int previousLength = refs.length;
                        refs = Arrays.copyOf(refs, ArrayUtil.oversize(count, RamUsageEstimator.NUM_BYTES_OBJECT_REF));
                        for (int i = previousLength; i < refs.length; ++i) {
                            refs[i] = new BytesRefBuilder();
                        }
                    }
                    for (int i = 0; i < count; ++i) {
                        final int length = in.readVInt();
                        final BytesRefBuilder scratch = refs[i];
                        scratch.grow(length);
                        in.readBytes(scratch.bytes(), 0, length);
                        scratch.setLength(length);
                    }
                }
            }

            @Override
            public int count() {
                return count;
            }

            @Override
            public BytesRef valueAt(int index) {
                return refs[index].get();
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
