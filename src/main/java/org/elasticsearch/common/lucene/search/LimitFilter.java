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

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.elasticsearch.common.lucene.docset.GetDocSet;

import java.io.IOException;

public class LimitFilter extends NoCacheFilter {

    private final int limit;
    private int counter;

    public LimitFilter(int limit) {
        this.limit = limit;
    }

    public int getLimit() {
        return limit;
    }

    @Override
    public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        if (counter > limit) {
            return null;
        }
        return new LimitDocSet(reader.maxDoc(), limit);
    }

    public class LimitDocSet extends GetDocSet {

        private final int limit;

        public LimitDocSet(int maxDoc, int limit) {
            super(maxDoc);
            this.limit = limit;
        }

        @Override
        public boolean get(int doc) {
            if (++counter > limit) {
                return false;
            }
            return true;
        }
    }
}