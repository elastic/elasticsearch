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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;

/**
 */
public class ParentChildAtomicFieldData implements AtomicFieldData {

    private final ImmutableOpenMap<String, PagedBytesAtomicFieldData> typeToIds;
    private final long numberUniqueValues;
    private final long memorySizeInBytes;

    public ParentChildAtomicFieldData(ImmutableOpenMap<String, PagedBytesAtomicFieldData> typeToIds) {
        this.typeToIds = typeToIds;
        long numValues = 0;
        for (ObjectCursor<PagedBytesAtomicFieldData> cursor : typeToIds.values()) {
            numValues += cursor.value.getNumberUniqueValues();
        }
        this.numberUniqueValues = numValues;
        long size = 0;
        for (ObjectCursor<PagedBytesAtomicFieldData> cursor : typeToIds.values()) {
            size += cursor.value.getMemorySizeInBytes();
        }
        this.memorySizeInBytes = size;
    }

    @Override
    public boolean isMultiValued() {
        return true;
    }

    @Override
    public long getNumberUniqueValues() {
        return numberUniqueValues;
    }

    @Override
    public long getMemorySizeInBytes() {
        return memorySizeInBytes;
    }

    @Override
    public BytesValues getBytesValues(boolean needsHashes) {
        final BytesValues[] bytesValues = new BytesValues[typeToIds.size()];
        int index = 0;
        for (ObjectCursor<PagedBytesAtomicFieldData> cursor : typeToIds.values()) {
            bytesValues[index++] = cursor.value.getBytesValues(needsHashes);
        }
        return new BytesValues(true) {

            private final BytesRef[] terms = new BytesRef[2];
            private int index;

            @Override
            public int setDocument(int docId) {
                index = 0;
                int counter = 0;
                for (final BytesValues values : bytesValues) {
                    int numValues = values.setDocument(docId);
                    assert numValues <= 1 : "Per doc/type combination only a single value is allowed";
                    if (numValues == 1) {
                        values.nextValue();
                        terms[counter++] = values.copyShared();
                    }
                }
                assert counter <= 2 : "A single doc can potentially be both parent and child, so the maximum allowed values is 2";
                if (counter > 1) {
                    int cmp = terms[0].compareTo(terms[1]);
                    if (cmp > 0) {
                        BytesRef temp = terms[0];
                        terms[0] = terms[1];
                        terms[1] = temp;
                    } else if (cmp == 0) {
                        // If the id is the same between types the only omit one. For example: a doc has parent#1 in _uid field and has grand_parent#1 in _parent field.
                        return 1;
                    }
                }
                return counter;
            }

            @Override
            public BytesRef nextValue() {
                BytesRef current = terms[index++];
                scratch.bytes = current.bytes;
                scratch.offset = current.offset;
                scratch.length = current.length;
                return scratch;
            }
        };
    }

    public BytesValues.WithOrdinals getBytesValues(String type) {
        WithOrdinals atomicFieldData = typeToIds.get(type);
        if (atomicFieldData != null) {
            return atomicFieldData.getBytesValues(true);
        } else {
            return null;
        }
    }

    public WithOrdinals getAtomicFieldData(String type) {
        return typeToIds.get(type);
    }

    @Override
    public ScriptDocValues getScriptValues() {
        return new ScriptDocValues.Strings(getBytesValues(false));
    }

    @Override
    public void close() {
        for (ObjectCursor<PagedBytesAtomicFieldData> cursor : typeToIds.values()) {
            cursor.value.close();
        }
    }
}
