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
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

/**
 */
public interface OrdinalsBytesValues extends BytesValues {

    Ordinals.Docs ordinals();

    BytesRef getValueByOrd(int ord);

    /**
     * Returns the bytes value for the docId, with the provided "ret" which will be filled with the
     * result which will also be returned. If there is no value for this docId, the length will be 0.
     * Note, the bytes are not "safe".
     */
    BytesRef getValueScratchByOrd(int ord, BytesRef ret);

    BytesRef getSafeValueByOrd(int ord);

    public static class StringBased extends BytesValues.StringBased implements OrdinalsBytesValues {

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
        public BytesRef getValueByOrd(int ord) {
            scratch.copyChars(values.getValueByOrd(ord));
            return scratch;
        }

        @Override
        public BytesRef getValueScratchByOrd(int ord, BytesRef ret) {
            ret.copyChars(values.getValueByOrd(ord));
            return ret;
        }

        @Override
        public BytesRef getSafeValueByOrd(int ord) {
            return new BytesRef(values.getValueByOrd(ord));
        }
    }
}
