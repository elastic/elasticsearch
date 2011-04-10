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

package org.elasticsearch.common.lucene;

import org.apache.lucene.index.ExtendedIndexSearcher;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.lease.Releasable;

/**
 * A very simple holder for a tuple of reader and searcher.
 *
 * @author kimchy (Shay Banon)
 */
public class ReaderSearcherHolder implements Releasable {

    private final ExtendedIndexSearcher indexSearcher;

    public ReaderSearcherHolder(IndexReader indexReader) {
        this(new ExtendedIndexSearcher(indexReader));
    }

    public ReaderSearcherHolder(ExtendedIndexSearcher indexSearcher) {
        this.indexSearcher = indexSearcher;
    }

    public IndexReader reader() {
        return indexSearcher.getIndexReader();
    }

    public ExtendedIndexSearcher searcher() {
        return indexSearcher;
    }

    @Override public boolean release() throws ElasticSearchException {
        try {
            indexSearcher.close();
        } catch (Exception e) {
            // do nothing
        }
        try {
            indexSearcher.getIndexReader().close();
        } catch (Exception e) {
            // do nothing
        }
        return true;
    }
}
