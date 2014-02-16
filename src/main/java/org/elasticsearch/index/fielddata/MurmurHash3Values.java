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

package org.elasticsearch.index.fielddata;

import com.carrotsearch.hppc.hash.MurmurHash3;
import org.apache.lucene.util.BytesRef;

/**
 *
 */
public class MurmurHash3Values {

    public static LongValues wrap(DoubleValues values) {
        return new Double(values);
    }

    public static LongValues wrap(LongValues values) {
        return new Long(values);
    }

    public static LongValues wrap(BytesValues values) {
        return new Bytes(values);
    }

    private static class Long extends LongValues {

        private final LongValues values;

        public Long(LongValues values) {
            super(values.isMultiValued());
            this.values = values;
        }

        @Override
        public int setDocument(int docId) {
            return values.setDocument(docId);
        }

        @Override
        public long nextValue() {
            return MurmurHash3.hash(values.nextValue());
        }

        @Override
        public AtomicFieldData.Order getOrder() {
            return AtomicFieldData.Order.NONE;
        }
    }

    private static class Double extends LongValues {

        private final DoubleValues values;

        public Double(DoubleValues values) {
            super(values.isMultiValued());
            this.values = values;
        }

        @Override
        public int setDocument(int docId) {
            return values.setDocument(docId);
        }

        @Override
        public long nextValue() {
            return MurmurHash3.hash(java.lang.Double.doubleToLongBits(values.nextValue()));
        }

        @Override
        public AtomicFieldData.Order getOrder() {
            return AtomicFieldData.Order.NONE;
        }
    }

    private static class Bytes extends LongValues {

        private final org.elasticsearch.common.hash.MurmurHash3.Hash128 hash = new org.elasticsearch.common.hash.MurmurHash3.Hash128();

        private final BytesValues values;

        public Bytes(BytesValues values) {
            super(values.isMultiValued());
            this.values = values;
        }

        @Override
        public int setDocument(int docId) {
            return values.setDocument(docId);
        }

        @Override
        public long nextValue() {
            final BytesRef next = values.nextValue();
            org.elasticsearch.common.hash.MurmurHash3.hash128(next.bytes, next.offset, next.length, 0, hash);
            return hash.h1;
        }

        @Override
        public AtomicFieldData.Order getOrder() {
            return AtomicFieldData.Order.NONE;
        }
    }
}
