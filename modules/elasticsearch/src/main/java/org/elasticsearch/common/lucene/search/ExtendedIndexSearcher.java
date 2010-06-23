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

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;

/**
 * @author kimchy (shay.banon)
 */
public class ExtendedIndexSearcher extends IndexSearcher {

    public ExtendedIndexSearcher(IndexSearcher searcher) {
        super(searcher.getIndexReader());
        setSimilarity(searcher.getSimilarity());
    }

    public ExtendedIndexSearcher(IndexReader r) {
        super(r);
    }

    public IndexReader[] subReaders() {
        return this.subReaders;
    }

    public int[] docStarts() {
        return this.docStarts;
    }

    // taken from DirectoryReader#readerIndex

    public int readerIndex(int doc) {
        int lo = 0;                                      // search starts array
        int hi = subReaders.length - 1;                  // for first element less

        while (hi >= lo) {
            int mid = (lo + hi) >>> 1;
            int midValue = docStarts[mid];
            if (doc < midValue)
                hi = mid - 1;
            else if (doc > midValue)
                lo = mid + 1;
            else {                                      // found a match
                while (mid + 1 < subReaders.length && docStarts[mid + 1] == midValue) {
                    mid++;                                  // scan to last match
                }
                return mid;
            }
        }
        return hi;
    }
}
