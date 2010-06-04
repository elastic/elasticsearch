/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.field;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.index.field.doubles.DoubleFieldData;
import org.elasticsearch.index.field.floats.FloatFieldData;
import org.elasticsearch.index.field.ints.IntFieldData;
import org.elasticsearch.index.field.longs.LongFieldData;
import org.elasticsearch.index.field.shorts.ShortFieldData;
import org.elasticsearch.index.field.strings.StringFieldData;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
// General TODOs on FieldData
// TODO Make storing of freqs optional
// TODO Optimize the order (both int[] and int[][] when they are sparse, create an Order abstraction)
public abstract class FieldData {

    public static enum Type {
        STRING(StringFieldData.class, false),
        SHORT(ShortFieldData.class, true),
        INT(IntFieldData.class, true),
        LONG(LongFieldData.class, true),
        FLOAT(FloatFieldData.class, true),
        DOUBLE(DoubleFieldData.class, true);

        public final Class<? extends FieldData> fieldDataClass;

        private final boolean isNumeric;

        Type(Class<? extends FieldData> clazz, boolean numeric) {
            this.fieldDataClass = clazz;
            this.isNumeric = numeric;
        }

        public boolean isNumeric() {
            return isNumeric;
        }
    }

    private final String fieldName;

    private final FieldDataOptions options;

    protected FieldData(String fieldName, FieldDataOptions options) {
        this.fieldName = fieldName;
        this.options = options;
    }

    /**
     * The field name of this field data.
     */
    public final String fieldName() {
        return fieldName;
    }

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
        void onValue(String value, int freq);
    }

    public abstract void forEachValueInDoc(int docId, StringValueInDocProc proc);

    public static interface StringValueInDocProc {
        void onValue(String value, int docId);
    }

    /**
     * The type of this field data.
     */
    public abstract Type type();

    public FieldDataOptions options() {
        return this.options;
    }

    public static FieldData load(Type type, IndexReader reader, String fieldName, FieldDataOptions options) throws IOException {
        return load(type.fieldDataClass, reader, fieldName, options);
    }

    @SuppressWarnings({"unchecked"})
    public static <T extends FieldData> T load(Class<T> type, IndexReader reader, String fieldName, FieldDataOptions options) throws IOException {
        if (type == StringFieldData.class) {
            return (T) StringFieldData.load(reader, fieldName, options);
        } else if (type == IntFieldData.class) {
            return (T) IntFieldData.load(reader, fieldName, options);
        } else if (type == LongFieldData.class) {
            return (T) LongFieldData.load(reader, fieldName, options);
        } else if (type == FloatFieldData.class) {
            return (T) FloatFieldData.load(reader, fieldName, options);
        } else if (type == DoubleFieldData.class) {
            return (T) DoubleFieldData.load(reader, fieldName, options);
        } else if (type == ShortFieldData.class) {
            return (T) ShortFieldData.load(reader, fieldName, options);
        }
        throw new ElasticSearchIllegalArgumentException("No support for type [" + type + "] to load field data");
    }
}
