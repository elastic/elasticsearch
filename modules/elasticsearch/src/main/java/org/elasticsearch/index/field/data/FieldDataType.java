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
import org.apache.lucene.search.FieldComparatorSource;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.bytes.ByteFieldDataType;
import org.elasticsearch.index.field.data.doubles.DoubleFieldDataType;
import org.elasticsearch.index.field.data.floats.FloatFieldDataType;
import org.elasticsearch.index.field.data.ints.IntFieldDataType;
import org.elasticsearch.index.field.data.longs.LongFieldDataType;
import org.elasticsearch.index.field.data.shorts.ShortFieldDataType;
import org.elasticsearch.index.field.data.strings.StringFieldDataType;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public interface FieldDataType<T extends FieldData> {

    public static final class DefaultTypes {
        public static final StringFieldDataType STRING = new StringFieldDataType();
        public static final ByteFieldDataType BYTE = new ByteFieldDataType();
        public static final ShortFieldDataType SHORT = new ShortFieldDataType();
        public static final IntFieldDataType INT = new IntFieldDataType();
        public static final LongFieldDataType LONG = new LongFieldDataType();
        public static final FloatFieldDataType FLOAT = new FloatFieldDataType();
        public static final DoubleFieldDataType DOUBLE = new DoubleFieldDataType();
    }

    FieldComparatorSource newFieldComparatorSource(FieldDataCache cache, @Nullable String missing);

    T load(IndexReader reader, String fieldName) throws IOException;
}
