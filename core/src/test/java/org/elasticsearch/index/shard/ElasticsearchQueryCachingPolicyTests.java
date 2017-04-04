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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ElasticsearchQueryCachingPolicyTests extends ESTestCase {

    public void testDoesNotCacheTermQueries() throws IOException {
        QueryCachingPolicy policy = QueryCachingPolicy.ALWAYS_CACHE;
        assertTrue(policy.shouldCache(new TermQuery(new Term("foo", "bar"))));
        assertTrue(policy.shouldCache(new PhraseQuery("foo", "bar", "baz")));
        policy = new ElasticsearchQueryCachingPolicy(policy);
        assertFalse(policy.shouldCache(new TermQuery(new Term("foo", "bar"))));
        assertTrue(policy.shouldCache(new PhraseQuery("foo", "bar", "baz")));
    }

    public void testDoesNotPutTermQueriesIntoTheHistory() {
        boolean[] used = new boolean[1];
        QueryCachingPolicy policy = new QueryCachingPolicy() {
            @Override
            public boolean shouldCache(Query query) throws IOException {
                throw new UnsupportedOperationException();
            }
            @Override
            public void onUse(Query query) {
                used[0] = true;
            }
        };
        policy = new ElasticsearchQueryCachingPolicy(policy);
        policy.onUse(new TermQuery(new Term("foo", "bar")));
        assertFalse(used[0]);
        policy.onUse(new PhraseQuery("foo", "bar", "baz"));
        assertTrue(used[0]);
    }

}
