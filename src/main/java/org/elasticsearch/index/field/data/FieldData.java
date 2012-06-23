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

package org.elasticsearch.index.field.data;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.util.concurrent.ThreadLocals;

import java.io.IOException;

/**
 *
 */
// General TODOs on FieldData
// TODO Optimize the order (both int[] and int[][] when they are sparse, create an Order abstraction)
public abstract class FieldData<Doc extends DocFieldData> {

    private final ThreadLocal<ThreadLocals.CleanableValue<Doc>> cachedDocFieldData = new ThreadLocal<ThreadLocals.CleanableValue<Doc>>() {
        @Override
        protected ThreadLocals.CleanableValue<Doc> initialValue() {
            return new ThreadLocals.CleanableValue<Doc>(createFieldData());
        }
    };

    private final String fieldName;

    private long sizeInBytes = -1;

    protected FieldData(String fieldName) {
        this.fieldName = fieldName;
    }

    /**
     * The field name of this field data.
     */
    public final String fieldName() {
        return fieldName;
    }

    public Doc docFieldData(int docId) {
        Doc docFieldData = cachedDocFieldData.get().get();
        docFieldData.setDocId(docId);
        return docFieldData;
    }

    public long sizeInBytes() {
        if (sizeInBytes == -1) {
            sizeInBytes = computeSizeInBytes();
        }
        return sizeInBytes;
    }

    protected abstract long computeSizeInBytes();

    protected abstract Doc createFieldData();

    /**
     * Is the field data a multi valued one (has multiple values / terms per document id) or not.
     */
    public abstract boolean multiValued();

    /**
     * Is there a value associated with this document id.
     */
    public abstract boolean hasValue(int docId);

    public abstract String stringValue(int docId);

    public abstract void forEachValue(StringValueProc proc);

    public static interface StringValueProc {
        void onValue(String value);
    }

    public abstract void forEachValueInDoc(int docId, StringValueInDocProc proc);

    public static interface StringValueInDocProc {
        void onValue(int docId, String value);

        void onMissing(int docId);
    }

    public abstract void forEachOrdinalInDoc(int docId, OrdinalInDocProc proc);

    public static interface OrdinalInDocProc {
        void onOrdinal(int docId, int ordinal);
    }

    /**
     * The type of this field data.
     */
    public abstract FieldDataType type();

    public static FieldData load(FieldDataType type, IndexReader reader, String fieldName) throws IOException {
        return type.load(reader, fieldName);
    }
}
