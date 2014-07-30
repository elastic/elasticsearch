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

package org.elasticsearch.index.cache.query.parser.resident;

import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParserSettings;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 */
public class ResidentQueryParserCacheTest extends ElasticsearchTestCase {

    @Test
    public void testCaching() throws Exception {
        ResidentQueryParserCache cache = new ResidentQueryParserCache(new Index("test"), ImmutableSettings.EMPTY);
        QueryParserSettings key = new QueryParserSettings();
        key.queryString("abc");
        key.defaultField("a");
        key.boost(2.0f);

        Query query = new TermQuery(new Term("a", "abc"));
        cache.put(key, query);

        assertThat(cache.get(key), not(sameInstance(query)));
        assertThat(cache.get(key), equalTo(query));
    }

}
