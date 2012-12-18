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

package org.elasticsearch.index.field.data.strings;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.support.FieldDataLoader;

import java.io.IOException;
import java.util.ArrayList;

/**
 *
 */
public abstract class StringFieldData extends FieldData<StringDocFieldData> {

    protected final BytesRef[] values;

    protected StringFieldData(String fieldName, BytesRef[] values) {
        super(fieldName);
        this.values = values;
    }

    @Override
    protected long computeSizeInBytes() {
        long size = RamUsage.NUM_BYTES_ARRAY_HEADER;
        for (BytesRef value : values) {
            if (value != null) {
                size += RamUsage.NUM_BYTES_OBJECT_REF + RamUsage.NUM_BYTES_OBJECT_HEADER +
                        RamUsage.NUM_BYTES_ARRAY_HEADER + (value.length + (2 * RamUsage.NUM_BYTES_INT));
            }
        }
        return size;
    }

    public BytesRef[] values() {
        return this.values;
    }

    abstract public BytesRef value(int docId);

    abstract public BytesRef[] values(int docId);

    @Override
    public StringDocFieldData docFieldData(int docId) {
        return super.docFieldData(docId);
    }

    @Override
    public BytesRef stringValue(int docId) {
        return value(docId);
    }

    @Override
    protected StringDocFieldData createFieldData() {
        return new StringDocFieldData(this);
    }

    @Override
    public FieldDataType type() {
        return FieldDataType.DefaultTypes.STRING;
    }

    @Override
    public void forEachValue(StringValueProc proc) {
        for (int i = 1; i < values.length; i++) {
            proc.onValue(values[i]);
        }
    }

    public static StringFieldData load(AtomicReader reader, String field) throws IOException {
        return FieldDataLoader.load(reader, field, new StringTypeLoader());
    }

    static class StringTypeLoader extends FieldDataLoader.FreqsTypeLoader<StringFieldData> {

        private final ArrayList<BytesRef> terms = new ArrayList<BytesRef>();

        StringTypeLoader() {
            super();
            // the first one indicates null value
            terms.add(null);
        }

        @Override
        public void collectTerm(BytesRef term) {
            terms.add(term);
        }

        @Override
        public StringFieldData buildSingleValue(String field, int[] ordinals) {
            return new SingleValueStringFieldData(field, ordinals, terms.toArray(new BytesRef[terms.size()]));
        }

        @Override
        public StringFieldData buildMultiValue(String field, int[][] ordinals) {
            return new MultiValueStringFieldData(field, ordinals, terms.toArray(new BytesRef[terms.size()]));
        }
    }
}
