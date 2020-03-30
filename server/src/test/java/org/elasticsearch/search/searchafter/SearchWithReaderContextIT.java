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

package org.elasticsearch.search.searchafter;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.search.ClearReaderAction;
import org.elasticsearch.action.search.ClearReaderRequest;
import org.elasticsearch.action.search.OpenReaderRequest;
import org.elasticsearch.action.search.OpenReaderResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportOpenReaderAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SearchWithReaderContextIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(randomIntBetween(100, 500)))
            .build();
    }

    public void testBasic() {
        createIndex("test");
        int numDocs = randomIntBetween(10, 50);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            client().prepareIndex("test").setId(id).setSource("value", i).get();
        }
        refresh("test");
        String readerId = openReaders(new String[]{"test"}, TimeValue.timeValueMinutes(2));
        SearchResponse resp1 = client().prepareSearch().setPreference(null).setReader(readerId, TimeValue.timeValueMinutes(2)).get();
        assertThat(resp1.getReaderId(), equalTo(readerId));
        assertHitCount(resp1, numDocs);
        int deletedDocs = 0;
        for (int i = 0; i < numDocs; i++) {
            if (randomBoolean()) {
                String id = Integer.toString(i);
                client().prepareDelete("test", id).get();
                deletedDocs++;
            }
        }
        refresh("test");
        if (randomBoolean()) {
            SearchResponse resp2 = client().prepareSearch("test").setPreference(null).setQuery(new MatchAllQueryBuilder()).get();
            assertNoFailures(resp2);
            assertHitCount(resp2, numDocs - deletedDocs);
        }
        try {
            SearchResponse resp3 = client().prepareSearch().setPreference(null).setQuery(new MatchAllQueryBuilder())
                .setReader(resp1.getReaderId(), TimeValue.timeValueMinutes(2))
                .get();
            assertNoFailures(resp3);
            assertHitCount(resp3, numDocs);
            assertThat(resp3.getReaderId(), equalTo(readerId));
        } finally {
            clearReaderId(readerId);
        }
    }

    public void testMultipleIndices() {
        int numIndices = randomIntBetween(1, 5);
        for (int i = 1; i <= numIndices; i++) {
            createIndex("index-" + i);
        }
        int numDocs = randomIntBetween(10, 50);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            String index = "index-" + randomIntBetween(1, numIndices);
            client().prepareIndex(index).setId(id).setSource("value", i).get();
        }
        refresh();
        String readerId = openReaders(new String[]{"*"}, TimeValue.timeValueMinutes(2));
        SearchResponse resp1 = client().prepareSearch().setPreference(null).setReader(readerId, TimeValue.timeValueMinutes(2)).get();
        assertNoFailures(resp1);
        assertHitCount(resp1, numDocs);
        int moreDocs = randomIntBetween(10, 50);
        for (int i = 0; i < moreDocs; i++) {
            String id = "more-" + i;
            String index = "index-" + randomIntBetween(1, numIndices);
            client().prepareIndex(index).setId(id).setSource("value", i).get();
        }
        refresh();
        try {
            SearchResponse resp2 = client().prepareSearch().get();
            assertNoFailures(resp2);
            assertHitCount(resp2, numDocs + moreDocs);

            SearchResponse resp3 = client().prepareSearch().setPreference(null)
                .setReader(resp1.getReaderId(), TimeValue.timeValueMinutes(1)).get();
            assertNoFailures(resp3);
            assertHitCount(resp3, numDocs);
        } finally {
            clearReaderId(resp1.getReaderId());
        }
    }

    public void testReaderIdNotFound() throws Exception {
        createIndex("index");
        int index1 = randomIntBetween(10, 50);
        for (int i = 0; i < index1; i++) {
            String id = Integer.toString(i);
            client().prepareIndex("index").setId(id).setSource("value", i).get();
        }
        refresh();
        String readerId = openReaders(new String[]{"index"}, TimeValue.timeValueSeconds(5));
        SearchResponse resp1 = client().prepareSearch().setPreference(null)
            .setReader(readerId, TimeValue.timeValueMillis(randomIntBetween(0, 10))).get();
        assertNoFailures(resp1);
        assertHitCount(resp1, index1);
        if (rarely()) {
            assertBusy(() -> {
                final CommonStats stats = client().admin().indices().prepareStats().setSearch(true).get().getTotal();
                assertThat(stats.search.getOpenContexts(), equalTo(0L));
            }, 60, TimeUnit.SECONDS);
        } else {
            clearReaderId(resp1.getReaderId());
        }
        SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class,
            () -> client().prepareSearch().setPreference(null).setReader(resp1.getReaderId(), TimeValue.timeValueMinutes(1)).get());
        for (ShardSearchFailure failure : e.shardFailures()) {
            assertThat(ExceptionsHelper.unwrapCause(failure.getCause()), instanceOf(SearchContextMissingException.class));
        }
    }

    public void testIndexNotFound() {
        createIndex("index-1");
        createIndex("index-2");

        int index1 = randomIntBetween(10, 50);
        for (int i = 0; i < index1; i++) {
            String id = Integer.toString(i);
            client().prepareIndex("index-1").setId(id).setSource("value", i).get();
        }

        int index2 = randomIntBetween(10, 50);
        for (int i = 0; i < index2; i++) {
            String id = Integer.toString(i);
            client().prepareIndex("index-2").setId(id).setSource("value", i).get();
        }
        refresh();
        String readerId = openReaders(new String[]{"index-*"}, TimeValue.timeValueMinutes(2));
        SearchResponse resp1 = client().prepareSearch().setPreference(null).setReader(readerId, TimeValue.timeValueMinutes(2)).get();
        assertNoFailures(resp1);
        assertHitCount(resp1, index1 + index2);
        client().admin().indices().prepareDelete("index-1").get();
        if (randomBoolean()) {
            SearchResponse resp2 = client().prepareSearch("index-*").get();
            assertNoFailures(resp2);
            assertHitCount(resp2, index2);

        }
        expectThrows(IndexNotFoundException.class, () -> client().prepareSearch()
            .setPreference(null)
            .setReader(resp1.getReaderId(), TimeValue.timeValueMinutes(1)).get());
        clearReaderId(resp1.getReaderId());
    }

    private String openReaders(String[] indices, TimeValue keepAlive) {
        OpenReaderRequest request = new OpenReaderRequest(indices, OpenReaderRequest.DEFAULT_INDICES_OPTIONS, keepAlive, null, null);
        final OpenReaderResponse response = client().execute(TransportOpenReaderAction.INSTANCE, request).actionGet();
        return response.getReaderId();
    }

    private void clearReaderId(String readerId) {
        client().execute(ClearReaderAction.INSTANCE, new ClearReaderRequest(readerId)).actionGet();
    }
}
