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

package org.elasticsearch.index.cache.filter.support;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.lucene.docset.DocSets;

import java.io.IOException;

public class FilterCacheValue<T> {

    private final T value;

    public FilterCacheValue(T value) {
        this.value = value;
    }

    public T value() {
        return value;
    }


    public static DocSet cacheable(IndexReader reader, DocIdSet set) throws IOException {
        if (set == null) {
            return DocSet.EMPTY_DOC_SET;
        }
        if (set == DocIdSet.EMPTY_DOCIDSET) {
            return DocSet.EMPTY_DOC_SET;
        }

        DocIdSetIterator it = set.iterator();
        if (it == null) {
            return DocSet.EMPTY_DOC_SET;
        }
        int doc = it.nextDoc();
        if (doc == DocIdSetIterator.NO_MORE_DOCS) {
            return DocSet.EMPTY_DOC_SET;
        }
        return DocSets.cacheable(reader, set);
    }
}