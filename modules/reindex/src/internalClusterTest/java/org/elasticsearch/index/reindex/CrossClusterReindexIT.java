/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class CrossClusterReindexIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER = "remote-cluster";

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return List.of(ReindexPlugin.class);
    }

    private int indexDocs(Client client, String index) {
        int numDocs = between(1, 100);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(index).setSource("f", "v").get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }

    public void testReindexFromRemoteGivenIndexExists() throws Exception {
        assertAcked(client(REMOTE_CLUSTER).admin().indices().prepareCreate("source-index-001"));
        final int docsNumber = indexDocs(client(REMOTE_CLUSTER), "source-index-001");

        final String sourceIndexInRemote = REMOTE_CLUSTER + ":" + "source-index-001";
        new ReindexRequestBuilder(client(LOCAL_CLUSTER), ReindexAction.INSTANCE).source(sourceIndexInRemote)
            .destination("desc-index-001")
            .ignoreUnavailable(randomBoolean()) // does not matter since source index exists
            .get();

        assertTrue("Number of documents in source and desc indexes does not match", waitUntil(() -> {
            SearchResponse resp = client(LOCAL_CLUSTER).prepareSearch("desc-index-001")
                .setQuery(new MatchAllQueryBuilder())
                .setSize(1000)
                .get();
            final TotalHits totalHits = resp.getHits().getTotalHits();
            return totalHits.relation == TotalHits.Relation.EQUAL_TO && totalHits.value == docsNumber;
        }));
    }

    public void testReindexFromRemoteGivenSameIndexNames() throws Exception {
        assertAcked(client(REMOTE_CLUSTER).admin().indices().prepareCreate("test-index-001"));
        final int docsNumber = indexDocs(client(REMOTE_CLUSTER), "test-index-001");

        final String sourceIndexInRemote = REMOTE_CLUSTER + ":" + "test-index-001";
        new ReindexRequestBuilder(client(LOCAL_CLUSTER), ReindexAction.INSTANCE).source(sourceIndexInRemote)
            .destination("test-index-001")
            .ignoreUnavailable(randomBoolean()) // does not matter since source index exists
            .get();

        assertTrue("Number of documents in source and desc indexes does not match", waitUntil(() -> {
            SearchResponse resp = client(LOCAL_CLUSTER).prepareSearch("test-index-001")
                .setQuery(new MatchAllQueryBuilder())
                .setSize(1000)
                .get();
            final TotalHits totalHits = resp.getHits().getTotalHits();
            return totalHits.relation == TotalHits.Relation.EQUAL_TO && totalHits.value == docsNumber;
        }));
    }

    public void testReindexManyTimesFromRemoteGivenSameIndexNames() throws Exception {
        assertAcked(client(REMOTE_CLUSTER).admin().indices().prepareCreate("test-index-001"));
        final long docsNumber = indexDocs(client(REMOTE_CLUSTER), "test-index-001");

        final String sourceIndexInRemote = REMOTE_CLUSTER + ":" + "test-index-001";

        int N = randomIntBetween(2, 10);
        for (int attempt = 0; attempt < N; attempt++) {

            BulkByScrollResponse response = new ReindexRequestBuilder(client(LOCAL_CLUSTER), ReindexAction.INSTANCE).source(sourceIndexInRemote)
                .destination("test-index-001")
                .ignoreUnavailable(randomBoolean())
                .get();

            if (attempt == 0) {
                assertThat(response.getCreated(), equalTo(docsNumber));
                assertThat(response.getUpdated(), equalTo(0L));
            } else {
                assertThat(response.getCreated(), equalTo(0L));
                assertThat(response.getUpdated(), equalTo(docsNumber));
            }

            assertTrue("Number of documents in source and desc indexes does not match", waitUntil(() -> {
                SearchResponse resp = client(LOCAL_CLUSTER).prepareSearch("test-index-001")
                    .setQuery(new MatchAllQueryBuilder())
                    .setSize(1000)
                    .get();
                final TotalHits totalHits = resp.getHits().getTotalHits();
                return totalHits.relation == TotalHits.Relation.EQUAL_TO && totalHits.value == docsNumber;
            }));
        }
    }

    public void testReindexFromRemoteIgnoreUnavailableIndex() {

        final String sourceIndexInRemote = REMOTE_CLUSTER + ":" + "no-such-source-index-001";
        BulkByScrollResponse response = new ReindexRequestBuilder(client(LOCAL_CLUSTER), ReindexAction.INSTANCE).source(sourceIndexInRemote)
            .destination("desc-index-001")
            .ignoreUnavailable(true)
            .get();

        // assert that nothing was copied over
        assertThat(response.getTotal(), equalTo(0L));
        assertThat(response.getUpdated(), equalTo(0L));
        assertThat(response.getCreated(), equalTo(0L));
        assertThat(response.getDeleted(), equalTo(0L));

        // assert that local index was not created either
        final IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> client(LOCAL_CLUSTER).prepareSearch("desc-index-001").setQuery(new MatchAllQueryBuilder()).setSize(1000).get()
        );

        assertThat(e, hasToString(containsString(String.format(Locale.ROOT, "no such index [%s]", "desc-index-001"))));
    }

    public void testReindexFromRemoteThrowOnUnavailableIndex() throws Exception {

        final String sourceIndexInRemote = REMOTE_CLUSTER + ":" + "no-such-source-index-001";
        expectThrows(
            IndexNotFoundException.class,
            () -> new ReindexRequestBuilder(client(LOCAL_CLUSTER), ReindexAction.INSTANCE).source(sourceIndexInRemote)
                .destination("desc-index-001")
                .ignoreUnavailable(false)
                .get()
        );

        // assert that local index was not created either
        final IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> client(LOCAL_CLUSTER).prepareSearch("desc-index-001").setQuery(new MatchAllQueryBuilder()).setSize(1000).get()
        );

        assertThat(e, hasToString(containsString(String.format(Locale.ROOT, "no such index [%s]", "desc-index-001"))));
    }

    public void testReindexFromRemoteGivenSimpleDateMathIndexName() throws InterruptedException {
        assertAcked(client(REMOTE_CLUSTER).admin().indices().prepareCreate("datemath-2001-01-02"));
        final int docsNumber = indexDocs(client(REMOTE_CLUSTER), "datemath-2001-01-02");

        final String sourceIndexInRemote = REMOTE_CLUSTER + ":" + "<datemath-{2001-01-01||+1d{yyyy-MM-dd}}>";
        new ReindexRequestBuilder(client(LOCAL_CLUSTER), ReindexAction.INSTANCE).source(sourceIndexInRemote)
            .destination("desc-index-001")
            .ignoreUnavailable(randomBoolean()) // does not matter since source index exists
            .get();

        assertTrue("Number of documents in source and desc indexes does not match", waitUntil(() -> {
            SearchResponse resp = client(LOCAL_CLUSTER).prepareSearch("desc-index-001")
                .setQuery(new MatchAllQueryBuilder())
                .setSize(1000)
                .get();
            final TotalHits totalHits = resp.getHits().getTotalHits();
            return totalHits.relation == TotalHits.Relation.EQUAL_TO && totalHits.value == docsNumber;
        }));
    }

    public void testReindexFromRemoteGivenComplexDateMathIndexName() throws InterruptedException {
        assertAcked(client(REMOTE_CLUSTER).admin().indices().prepareCreate("datemath-2001-01-01-14"));
        final int docsNumber = indexDocs(client(REMOTE_CLUSTER), "datemath-2001-01-01-14");

        // Remote name contains `:` symbol twice
        final String sourceIndexInRemote = REMOTE_CLUSTER + ":" + "<datemath-{2001-01-01-13||+1h/h{yyyy-MM-dd-HH|-07:00}}>";
        new ReindexRequestBuilder(client(LOCAL_CLUSTER), ReindexAction.INSTANCE).source(sourceIndexInRemote)
            .destination("desc-index-001")
            .ignoreUnavailable(randomBoolean()) // does not matter since source index exists
            .get();

        assertTrue("Number of documents in source and desc indexes does not match", waitUntil(() -> {
            SearchResponse resp = client(LOCAL_CLUSTER).prepareSearch("desc-index-001")
                .setQuery(new MatchAllQueryBuilder())
                .setSize(1000)
                .get();
            final TotalHits totalHits = resp.getHits().getTotalHits();
            return totalHits.relation == TotalHits.Relation.EQUAL_TO && totalHits.value == docsNumber;
        }));
    }

}
