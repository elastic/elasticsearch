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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

abstract class  AbstractBinaryDVLeafFieldData implements LeafFieldData {
    private final BinaryDocValues values;

    AbstractBinaryDVLeafFieldData(BinaryDocValues values) {
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
            final ByteArrayDataInput in = new ByteArrayDataInput();
            final BytesRef scratch = new BytesRef();

            @Override
            public boolean advanceExact(int doc) throws IOException {
                if (values.advanceExact(doc)) {
                    final BytesRef bytes = values.binaryValue();
                    assert bytes.length > 0;
                    in.reset(bytes.bytes, bytes.offset, bytes.length);
                    count = in.readVInt();
                    scratch.bytes = bytes.bytes;
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            public int docValueCount() {
                return count;
            }

            @Override
            public BytesRef nextValue() throws IOException {
                scratch.length = in.readVInt();
                scratch.offset = in.getPosition();
                in.setPosition(scratch.offset + scratch.length);
                return scratch;
            }

        };
    }


    @Override
    public void close() {
        // no-op
    }
}