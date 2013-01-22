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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.index.fielddata.ordinals.EmptyOrdinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

/**
 */
public interface OrdinalsHashedBytesValues extends HashedBytesValues {

    Ordinals.Docs ordinals();

    HashedBytesRef getValueByOrd(int ord);

    HashedBytesRef getSafeValueByOrd(int ord);

    public static class Empty extends HashedBytesValues.Empty implements OrdinalsHashedBytesValues {

        private final Ordinals ordinals;

        public Empty(EmptyOrdinals ordinals) {
            this.ordinals = ordinals;
        }

        @Override
        public Ordinals.Docs ordinals() {
            return ordinals.ordinals();
        }

        @Override
        public HashedBytesRef getValueByOrd(int ord) {
            return null;
        }

        @Override
        public HashedBytesRef getSafeValueByOrd(int ord) {
            return null;
        }
    }

    static class BytesBased extends HashedBytesValues.BytesBased implements OrdinalsHashedBytesValues {

        private final OrdinalsBytesValues values;

        public BytesBased(OrdinalsBytesValues values) {
            super(values);
            this.values = values;
        }

        @Override
        public Ordinals.Docs ordinals() {
            return values.ordinals();
        }

        @Override
        public HashedBytesRef getValueByOrd(int ord) {
            scratch.bytes = values.getValueByOrd(ord);
            return scratch.resetHashCode();
        }

        @Override
        public HashedBytesRef getSafeValueByOrd(int ord) {
            return new HashedBytesRef(values.getSafeValueByOrd(ord));
        }
    }

    static class StringBased extends HashedBytesValues.StringBased implements OrdinalsHashedBytesValues {

        private final OrdinalsStringValues values;

        public StringBased(OrdinalsStringValues values) {
            super(values);
            this.values = values;
        }

        @Override
        public Ordinals.Docs ordinals() {
            return values.ordinals();
        }

        @Override
        public HashedBytesRef getValueByOrd(int ord) {
            scratch.bytes.copyChars(values.getValueByOrd(ord));
            return scratch.resetHashCode();
        }

        @Override
        public HashedBytesRef getSafeValueByOrd(int ord) {
            return new HashedBytesRef(new BytesRef(values.getValueByOrd(ord)));
        }
    }
}
