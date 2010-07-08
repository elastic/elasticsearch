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

package org.elasticsearch.index.field.data;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.thread.ThreadLocals;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.doubles.DoubleFieldData;
import org.elasticsearch.index.field.data.doubles.DoubleFieldDataComparator;
import org.elasticsearch.index.field.data.floats.FloatFieldData;
import org.elasticsearch.index.field.data.floats.FloatFieldDataComparator;
import org.elasticsearch.index.field.data.ints.IntFieldData;
import org.elasticsearch.index.field.data.ints.IntFieldDataComparator;
import org.elasticsearch.index.field.data.longs.LongFieldData;
import org.elasticsearch.index.field.data.longs.LongFieldDataComparator;
import org.elasticsearch.index.field.data.shorts.ShortFieldData;
import org.elasticsearch.index.field.data.shorts.ShortFieldDataComparator;
import org.elasticsearch.index.field.data.strings.StringFieldData;
import org.elasticsearch.index.field.data.strings.StringOrdValFieldDataComparator;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
// General TODOs on FieldData
// TODO Optimize the order (both int[] and int[][] when they are sparse, create an Order abstraction)
public abstract class FieldData<Doc extends DocFieldData> {

    public static enum Type {
        STRING() {

            @Override public Class<? extends FieldData> fieldDataClass() {
                return StringFieldData.class;
            }

            @Override public boolean isNumeric() {
                return false;
            }

            @Override public FieldComparatorSource newFieldComparatorSource(final FieldDataCache cache) {
                return new FieldComparatorSource() {
                    @Override public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
                        return new StringOrdValFieldDataComparator(numHits, fieldname, sortPos, reversed, cache);
                    }
                };
            }},
        SHORT() {
            @Override public Class<? extends FieldData> fieldDataClass() {
                return ShortFieldData.class;
            }

            @Override public boolean isNumeric() {
                return true;
            }

            @Override public FieldComparatorSource newFieldComparatorSource(final FieldDataCache cache) {
                return new FieldComparatorSource() {
                    @Override public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
                        return new ShortFieldDataComparator(numHits, fieldname, cache);
                    }
                };
            }},
        INT() {
            @Override public Class<? extends FieldData> fieldDataClass() {
                return IntFieldData.class;
            }

            @Override public boolean isNumeric() {
                return true;
            }

            @Override public FieldComparatorSource newFieldComparatorSource(final FieldDataCache cache) {
                return new FieldComparatorSource() {
                    @Override public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
                        return new IntFieldDataComparator(numHits, fieldname, cache);
                    }
                };
            }},
        LONG() {
            @Override public Class<? extends FieldData> fieldDataClass() {
                return LongFieldData.class;
            }

            @Override public boolean isNumeric() {
                return true;
            }

            @Override public FieldComparatorSource newFieldComparatorSource(final FieldDataCache cache) {
                return new FieldComparatorSource() {
                    @Override public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
                        return new LongFieldDataComparator(numHits, fieldname, cache);
                    }
                };
            }},
        FLOAT() {
            @Override public Class<? extends FieldData> fieldDataClass() {
                return FloatFieldData.class;
            }

            @Override public boolean isNumeric() {
                return true;
            }

            @Override public FieldComparatorSource newFieldComparatorSource(final FieldDataCache cache) {
                return new FieldComparatorSource() {
                    @Override public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
                        return new FloatFieldDataComparator(numHits, fieldname, cache);
                    }
                };
            }},
        DOUBLE() {
            @Override public Class<? extends FieldData> fieldDataClass() {
                return DoubleFieldData.class;
            }

            @Override public boolean isNumeric() {
                return true;
            }

            @Override public FieldComparatorSource newFieldComparatorSource(final FieldDataCache cache) {
                return new FieldComparatorSource() {
                    @Override public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
                        return new DoubleFieldDataComparator(numHits, fieldname, cache);
                    }
                };
            }};

        public abstract Class<? extends FieldData> fieldDataClass();

        public abstract boolean isNumeric();

        public abstract FieldComparatorSource newFieldComparatorSource(FieldDataCache cache);
    }

    private final ThreadLocal<ThreadLocals.CleanableValue<Doc>> cachedDocFieldData = new ThreadLocal<ThreadLocals.CleanableValue<Doc>>() {
        @Override protected ThreadLocals.CleanableValue<Doc> initialValue() {
            return new ThreadLocals.CleanableValue<Doc>(createFieldData());
        }
    };

    private final String fieldName;

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
    }

    /**
     * The type of this field data.
     */
    public abstract Type type();

    public abstract FieldComparator newComparator(FieldDataCache fieldDataCache, int numHits, String field, int sortPos, boolean reversed);

    public static FieldData load(Type type, IndexReader reader, String fieldName) throws IOException {
        return load(type.fieldDataClass(), reader, fieldName);
    }

    @SuppressWarnings({"unchecked"})
    public static <T extends FieldData> T load(Class<T> type, IndexReader reader, String fieldName) throws IOException {
        if (type == StringFieldData.class) {
            return (T) StringFieldData.load(reader, fieldName);
        } else if (type == IntFieldData.class) {
            return (T) IntFieldData.load(reader, fieldName);
        } else if (type == LongFieldData.class) {
            return (T) LongFieldData.load(reader, fieldName);
        } else if (type == FloatFieldData.class) {
            return (T) FloatFieldData.load(reader, fieldName);
        } else if (type == DoubleFieldData.class) {
            return (T) DoubleFieldData.load(reader, fieldName);
        } else if (type == ShortFieldData.class) {
            return (T) ShortFieldData.load(reader, fieldName);
        }
        throw new ElasticSearchIllegalArgumentException("No support for type [" + type + "] to load field data");
    }
}
