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

package org.elasticsearch.util.lucene.docidset;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.OpenBitSetDISI;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class DocIdSets {

    /**
     * Returns a cacheable version of the doc id set (might be the same instance provided as a parameter).
     */
    public static DocIdSet cacheable(IndexReader reader, DocIdSet docIdSet) throws IOException {
        if (docIdSet.isCacheable()) {
            return docIdSet;
        } else {
            final DocIdSetIterator it = docIdSet.iterator();
            // null is allowed to be returned by iterator(),
            // in this case we wrap with the empty set,
            // which is cacheable.
            return (it == null) ? DocIdSet.EMPTY_DOCIDSET : new OpenBitSetDISI(it, reader.maxDoc());
        }
    }

    private DocIdSets() {

    }

}
