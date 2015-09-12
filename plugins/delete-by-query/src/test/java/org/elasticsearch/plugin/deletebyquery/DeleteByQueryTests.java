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

package org.elasticsearch.plugin.deletebyquery;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.IndexDeleteByQueryResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = SUITE, transportClientRatio = 0)
public class DeleteByQueryTests extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(DeleteByQueryPlugin.class);
    }

    @Test(expected = ActionRequestValidationException.class)
    public void testDeleteByQueryWithNoSource() {
        newDeleteByQuery().get();
        fail("should have thrown a validation exception because of the missing source");
    }

    @Test
    public void testDeleteByQueryWithNoIndices() {
        DeleteByQueryRequestBuilder delete = newDeleteByQuery().setQuery(QueryBuilders.matchAllQuery());
        delete.setIndicesOptions(IndicesOptions.fromOptions(false, true, true, false));
        assertDBQResponse(delete.get(), 0L, 0l, 0l, 0l);
        assertSearchContextsClosed();
    }

    @Test
    public void testDeleteByQueryWithOneIndex() throws Exception {
        final long docs = randomIntBetween(1, 50);
        for (int i = 0; i < docs; i++) {
            index("test", "test", String.valueOf(i), "fields1", 1);
        }
        refresh();
        assertHitCount(client().prepareCount("test").get(), docs);

        DeleteByQueryRequestBuilder delete = newDeleteByQuery().setIndices("t*").setQuery(QueryBuilders.matchAllQuery());
        assertDBQResponse(delete.get(), docs, docs, 0l, 0l);
        refresh();
        assertHitCount(client().prepareCount("test").get(), 0);
        assertSearchContextsClosed();
    }

    @Test
    public void testDeleteByQueryWithMultipleIndices() throws Exception {
        final int indices = randomIntBetween(2, 5);
        final int docs = randomIntBetween(2, 10) * 2;
        long[] candidates = new long[indices];

        for (int i = 0; i < indices; i++) {
            // number of documents to be deleted with the upcoming delete-by-query
            // (this number differs for each index)
            candidates[i] = randomIntBetween(1, docs);

            for (int j = 0; j < docs; j++) {
                boolean candidate = (j < candidates[i]);
                index("test-" + i, "test", String.valueOf(j), "candidate", candidate);
            }
        }

        // total number of expected deletions
        long deletions = 0;
        for (long i : candidates) {
            deletions = deletions + i;
        }
        refresh();

        assertHitCount(client().prepareCount().get(), docs * indices);
        for (int i = 0; i < indices; i++) {
            assertHitCount(client().prepareCount("test-" + i).get(), docs);
        }

        // Deletes all the documents with candidate=true
        DeleteByQueryResponse response = newDeleteByQuery().setIndices("test-*").setQuery(QueryBuilders.termQuery("candidate", true)).get();
        refresh();

        // Checks that the DBQ response returns the expected number of deletions
        assertDBQResponse(response, deletions, deletions, 0l, 0l);
        assertNotNull(response.getIndices());
        assertThat(response.getIndices().length, equalTo(indices));

        for (int i = 0; i < indices; i++) {
            String indexName = "test-" + i;
            IndexDeleteByQueryResponse indexResponse = response.getIndex(indexName);
            assertThat(indexResponse.getFound(), equalTo(candidates[i]));
            assertThat(indexResponse.getDeleted(), equalTo(candidates[i]));
            assertThat(indexResponse.getFailed(), equalTo(0L));
            assertThat(indexResponse.getMissing(), equalTo(0L));
            assertThat(indexResponse.getIndex(), equalTo(indexName));
            long remaining = docs - candidates[i];
            assertHitCount(client().prepareCount(indexName).get(), remaining);
        }

        assertHitCount(client().prepareCount().get(), (indices * docs) - deletions);
        assertSearchContextsClosed();
    }

    @Test
    public void testDeleteByQueryWithMissingIndex() throws Exception {
        client().prepareIndex("test", "test")
                .setSource(jsonBuilder().startObject().field("field1", 1).endObject())
                .setRefresh(true)
                .get();
        assertHitCount(client().prepareCount().get(), 1);

        DeleteByQueryRequestBuilder delete = newDeleteByQuery().setIndices("test", "missing").setQuery(QueryBuilders.matchAllQuery());
        try {
            delete.get();
            fail("should have thrown an exception because of a missing index");
        } catch (IndexNotFoundException e) {
            // Ok
        }

        delete.setIndicesOptions(IndicesOptions.lenientExpandOpen());
        assertDBQResponse(delete.get(), 1L, 1L, 0l, 0l);
        refresh();
        assertHitCount(client().prepareCount("test").get(), 0);
        assertSearchContextsClosed();
    }

    @Test
    public void testDeleteByQueryWithTypes() throws Exception {
        final long docs = randomIntBetween(1, 50);
        for (int i = 0; i < docs; i++) {
            index(randomFrom("test1", "test2", "test3"), "type1", String.valueOf(i), "foo", "bar");
            index(randomFrom("test1", "test2", "test3"), "type2", String.valueOf(i), "foo", "bar");
        }
        refresh();
        assertHitCount(client().prepareCount().get(), docs * 2);
        assertHitCount(client().prepareCount().setTypes("type1").get(), docs);
        assertHitCount(client().prepareCount().setTypes("type2").get(), docs);

        DeleteByQueryRequestBuilder delete = newDeleteByQuery().setTypes("type1").setQuery(QueryBuilders.matchAllQuery());
        assertDBQResponse(delete.get(), docs, docs, 0l, 0l);
        refresh();

        assertHitCount(client().prepareCount().get(), docs);
        assertHitCount(client().prepareCount().setTypes("type1").get(), 0);
        assertHitCount(client().prepareCount().setTypes("type2").get(), docs);
        assertSearchContextsClosed();
    }

    @Test
    public void testDeleteByQueryWithRouting() throws Exception {
        assertAcked(prepareCreate("test").setSettings("number_of_shards", 2));
        ensureGreen("test");

        final int docs = randomIntBetween(2, 10);
        logger.info("--> indexing [{}] documents with routing", docs);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex("test", "test", String.valueOf(i)).setRouting(String.valueOf(i)).setSource("field1", 1).get();
        }
        refresh();

        logger.info("--> counting documents with no routing, should be equal to [{}]", docs);
        assertHitCount(client().prepareCount().get(), docs);

        String routing = String.valueOf(randomIntBetween(2, docs));

        logger.info("--> counting documents with routing [{}]", routing);
        long expected = client().prepareCount().setRouting(routing).get().getCount();

        logger.info("--> delete all documents with routing [{}] with a delete-by-query", routing);
        DeleteByQueryRequestBuilder delete = newDeleteByQuery().setRouting(routing).setQuery(QueryBuilders.matchAllQuery());
        assertDBQResponse(delete.get(), expected, expected, 0l, 0l);
        refresh();

        assertHitCount(client().prepareCount().get(), docs - expected);
        assertSearchContextsClosed();
    }

    @Test
    public void testDeleteByFieldQuery() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")));

        int numDocs = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test", "test", Integer.toString(i))
                    .setRouting(randomAsciiOfLengthBetween(1, 5))
                    .setSource("foo", "bar").get();
        }
        refresh();

        int n = between(0, numDocs - 1);
        assertHitCount(client().prepareCount("test").setQuery(QueryBuilders.matchQuery("_id", Integer.toString(n))).get(), 1);
        assertHitCount(client().prepareCount("test").setQuery(QueryBuilders.matchAllQuery()).get(), numDocs);

        DeleteByQueryRequestBuilder delete = newDeleteByQuery().setIndices("alias").setQuery(QueryBuilders.matchQuery("_id", Integer.toString(n)));
        assertDBQResponse(delete.get(), 1L, 1L, 0l, 0l);
        refresh();
        assertHitCount(client().prepareCount("test").setQuery(QueryBuilders.matchAllQuery()).get(), numDocs - 1);
        assertSearchContextsClosed();
    }

    @Test
    public void testDeleteByQueryWithDateMath() throws Exception {
        index("test", "type", "1", "d", "2013-01-01");
        ensureGreen();
        refresh();
        assertHitCount(client().prepareCount("test").get(), 1);

        DeleteByQueryRequestBuilder delete = newDeleteByQuery().setIndices("test").setQuery(QueryBuilders.rangeQuery("d").to("now-1h"));
        assertDBQResponse(delete.get(), 1L, 1L, 0l, 0l);
        refresh();
        assertHitCount(client().prepareCount("test").get(), 0);
        assertSearchContextsClosed();
    }

    @Test
    public void testDeleteByTermQuery() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = scaledRandomIntBetween(10, 50);
        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[numDocs + 1];
        for (int i = 0; i < numDocs; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "test", Integer.toString(i)).setSource("field", "value");
        }
        indexRequestBuilders[numDocs] = client().prepareIndex("test", "test", Integer.toString(numDocs)).setSource("field", "other_value");
        indexRandom(true, indexRequestBuilders);

        SearchResponse searchResponse = client().prepareSearch("test").get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo((long) numDocs + 1));

        DeleteByQueryResponse delete = newDeleteByQuery().setIndices("test").setQuery(QueryBuilders.termQuery("field", "value")).get();
        assertDBQResponse(delete, numDocs, numDocs, 0l, 0l);

        refresh();
        searchResponse = client().prepareSearch("test").get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertSearchContextsClosed();
    }

    @Test

    public void testConcurrentDeleteByQueriesOnDifferentDocs() throws InterruptedException {
        createIndex("test");
        ensureGreen();

        final Thread[] threads =  new Thread[scaledRandomIntBetween(2, 5)];
        final long docs = randomIntBetween(1, 50);
        for (int i = 0; i < docs; i++) {
            for (int j = 0; j < threads.length; j++) {
                index("test", "test", String.valueOf(i * 10 + j), "field", j);
            }
        }
        refresh();
        assertHitCount(client().prepareCount("test").get(), docs * threads.length);

        final CountDownLatch start = new CountDownLatch(1);
        final AtomicReference<Throwable> exceptionHolder = new AtomicReference<>();

        for (int i = 0; i < threads.length; i++) {
            final int threadNum = i;
            assertHitCount(client().prepareCount("test").setQuery(QueryBuilders.termQuery("field", threadNum)).get(), docs);

            Runnable r =  new Runnable() {
                @Override
                public void run() {
                    try {
                        start.await();

                        DeleteByQueryResponse rsp = newDeleteByQuery().setQuery(QueryBuilders.termQuery("field", threadNum)).get();
                        assertDBQResponse(rsp, docs, docs, 0L, 0L);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Throwable e) {
                        exceptionHolder.set(e);
                        Thread.currentThread().interrupt();
                    }
                }
            };
            threads[i] = new Thread(r);
            threads[i].start();
        }

        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        Throwable assertionError = exceptionHolder.get();
        if (assertionError != null) {
            assertionError.printStackTrace();
        }
        assertThat(assertionError + " should be null", assertionError, nullValue());
        refresh();

        for (int i = 0; i < threads.length; i++) {
            assertHitCount(client().prepareCount("test").setQuery(QueryBuilders.termQuery("field", i)).get(), 0);
        }
        assertSearchContextsClosed();
    }

    @Test
    public void testConcurrentDeleteByQueriesOnSameDocs() throws InterruptedException {
        assertAcked(prepareCreate("test").setSettings(Settings.settingsBuilder().put("index.refresh_interval", -1)));
        ensureGreen();

        final long docs = randomIntBetween(50, 100);
        for (int i = 0; i < docs; i++) {
            index("test", "test", String.valueOf(i), "foo", "bar");
        }
        refresh();
        assertHitCount(client().prepareCount("test").get(), docs);

        final Thread[] threads =  new Thread[scaledRandomIntBetween(2, 9)];

        final CountDownLatch start = new CountDownLatch(1);
        final AtomicReference<Throwable> exceptionHolder = new AtomicReference<>();

        final MatchQueryBuilder query = QueryBuilders.matchQuery("foo", "bar");
        final AtomicLong deleted = new AtomicLong(0);

        for (int i = 0; i < threads.length; i++) {
            assertHitCount(client().prepareCount("test").setQuery(query).get(), docs);

            Runnable r =  new Runnable() {
                @Override
                public void run() {
                    try {
                        start.await();
                        DeleteByQueryResponse rsp = newDeleteByQuery().setQuery(query).get();
                        deleted.addAndGet(rsp.getTotalDeleted());

                        assertThat(rsp.getTotalFound(), equalTo(docs));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Throwable e) {
                        exceptionHolder.set(e);
                        Thread.currentThread().interrupt();
                    }
                }
            };
            threads[i] = new Thread(r);
            threads[i].start();
        }

        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
        refresh();

        Throwable assertionError = exceptionHolder.get();
        if (assertionError != null) {
            assertionError.printStackTrace();
        }
        assertThat(assertionError + " should be null", assertionError, nullValue());
        assertHitCount(client().prepareCount("test").get(), 0L);
        assertThat(deleted.get(), equalTo(docs));
        assertSearchContextsClosed();
    }

    @Test
    public void testDeleteByQueryOnReadOnlyIndex() throws InterruptedException {
        createIndex("test");
        ensureGreen();

        final long docs = randomIntBetween(1, 50);
        for (int i = 0; i < docs; i++) {
            index("test", "test", String.valueOf(i), "field", 1);
        }
        refresh();
        assertHitCount(client().prepareCount("test").get(), docs);

        try {
            enableIndexBlock("test", IndexMetaData.SETTING_READ_ONLY);
            DeleteByQueryResponse rsp = newDeleteByQuery().setQuery(QueryBuilders.matchAllQuery()).get();
            assertDBQResponse(rsp, docs, 0L, docs, 0L);
        } finally {
            disableIndexBlock("test", IndexMetaData.SETTING_READ_ONLY);
        }

        assertHitCount(client().prepareCount("test").get(), docs);
        assertSearchContextsClosed();
    }

    private DeleteByQueryRequestBuilder newDeleteByQuery() {
        return new DeleteByQueryRequestBuilder(client(), DeleteByQueryAction.INSTANCE);
    }

    private void assertDBQResponse(DeleteByQueryResponse response, long found, long deleted, long failed, long missing) {
        assertNotNull(response);
        assertThat(response.isTimedOut(), equalTo(false));
        assertThat(response.getShardFailures().length, equalTo(0));
        assertThat(response.getTotalFound(), equalTo(found));
        assertThat(response.getTotalDeleted(), equalTo(deleted));
        assertThat(response.getTotalFailed(), equalTo(failed));
        assertThat(response.getTotalMissing(), equalTo(missing));
    }

    private void assertSearchContextsClosed() {
        NodesStatsResponse nodesStats = client().admin().cluster().prepareNodesStats().setIndices(true).get();
        for (NodeStats nodeStat : nodesStats.getNodes()){
            assertThat(nodeStat.getIndices().getSearch().getOpenContexts(), equalTo(0L));
        }
    }
}