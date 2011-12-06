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

package org.elasticsearch.common.lucene.docset;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;

import java.io.IOException;

/**
 *
 */
public abstract class DocSet extends DocIdSet implements Bits {

    public static final DocSet EMPTY_DOC_SET = new DocSet() {
        @Override
        public boolean get(int doc) {
            return false;
        }

        @Override
        public DocIdSetIterator iterator() throws IOException {
            return DocIdSet.EMPTY_DOCIDSET.iterator();
        }

        @Override
        public boolean isCacheable() {
            return true;
        }

        @Override
        public long sizeInBytes() {
            return 0;
        }

        @Override
        public int length() {
            return 0;
        }
    };

    public abstract boolean get(int doc);

    public abstract long sizeInBytes();
}
