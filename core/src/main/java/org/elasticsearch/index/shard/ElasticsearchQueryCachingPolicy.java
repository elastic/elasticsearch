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

package org.elasticsearch.index.shard;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.TermQuery;

import java.io.IOException;

/**
 * A {@link QueryCachingPolicy} that does not cache {@link TermQuery}s.
 */
final class ElasticsearchQueryCachingPolicy implements QueryCachingPolicy {

    private final QueryCachingPolicy in;

    ElasticsearchQueryCachingPolicy(QueryCachingPolicy in) {
        this.in = in;
    }

    @Override
    public void onUse(Query query) {
        if (query.getClass() != TermQuery.class) {
            // Do not waste space in the history for term queries. The assumption
            // is that these queries are very fast so not worth caching
            in.onUse(query);
        }
    }

    @Override
    public boolean shouldCache(Query query) throws IOException {
        if (query.getClass() == TermQuery.class) {
            return false;
        }
        return in.shouldCache(query);
    }

}
