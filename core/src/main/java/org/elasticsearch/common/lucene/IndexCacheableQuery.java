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

package org.elasticsearch.common.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;

/**
 * Base implementation for a query which is cacheable at the index level but
 * not the segment level as usually expected.
 */
public abstract class IndexCacheableQuery extends Query {

    private Object readerCacheKey;

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        if (reader.getCoreCacheKey() != this.readerCacheKey) {
            IndexCacheableQuery rewritten = (IndexCacheableQuery) clone();
            rewritten.readerCacheKey = reader.getCoreCacheKey();
            return rewritten;
        }
        return super.rewrite(reader);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj)
                && readerCacheKey == ((IndexCacheableQuery) obj).readerCacheKey;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(readerCacheKey);
    }

    @Override
    public final Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        if (readerCacheKey == null) {
            throw new IllegalStateException("Rewrite first");
        }
        if (readerCacheKey != searcher.getIndexReader().getCoreCacheKey()) {
            throw new IllegalStateException("Must create weight on the same reader which has been used for rewriting");
        }
        return doCreateWeight(searcher, needsScores);
    }

    /** Create a {@link Weight} for this query.
     *  @see Query#createWeight(IndexSearcher, boolean)
     */
    public abstract Weight doCreateWeight(IndexSearcher searcher, boolean needsScores) throws IOException;
}
