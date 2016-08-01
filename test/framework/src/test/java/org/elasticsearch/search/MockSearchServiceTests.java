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

package org.elasticsearch.search;

import org.apache.lucene.search.Query;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;

public class MockSearchServiceTests extends ESTestCase {
    public void testAssertNoInFlightContext() {
        SearchContext s = new TestSearchContext(new QueryShardContext(new IndexSettings(IndexMetaData.PROTO, Settings.EMPTY), null, null,
                null, null, null, null, null, null, null)) {
            @Override
            public SearchShardTarget shardTarget() {
                return new SearchShardTarget("node", new Index("idx", "ignored"), 0);
            }

            @Override
            public SearchType searchType() {
                return SearchType.DEFAULT;
            }

            @Override
            public Query query() {
                return Queries.newMatchAllQuery();
            }
        };
        MockSearchService.addActiveContext(s);
        try {
            Throwable e = expectThrows(AssertionError.class, () -> MockSearchService.assertNoInFlightContext());
            assertEquals("There are still [1] in-flight contexts. The first one's creation site is listed as the cause of this exception.",
                    e.getMessage());
            e = e.getCause();
            // The next line with throw an exception if the date looks wrong
            assertEquals("[node][idx][0] query=[*:*]", e.getMessage());
            assertEquals(MockSearchService.class.getName(), e.getStackTrace()[0].getClassName());
            assertEquals(MockSearchServiceTests.class.getName(), e.getStackTrace()[1].getClassName());
        } finally {
            MockSearchService.removeActiveContext(s);
        }
    }
}
