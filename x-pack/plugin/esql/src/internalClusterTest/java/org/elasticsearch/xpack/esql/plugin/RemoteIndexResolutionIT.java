/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractCrossClusterTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RemoteIndexResolutionIT extends AbstractCrossClusterTestCase {

    public void testResolvesRemoteIndex() {
        indexRandom(REMOTE_CLUSTER_1, true, "index-1", 1);

        try (var response = run(syncEsqlQueryRequest().query("FROM " + REMOTE_CLUSTER_1 + ":index-1 METADATA _index"))) {
            assertOk(response);
            assertResultConcreteIndices(response, REMOTE_CLUSTER_1 + ":index-1");
        }
    }

    public void testResolveRemoteUnknownIndex() {
        // Today we do not allow empty index resolution.
        // This index is mixed into the resultset to test error handling of the missing concrete remote, not the empty result.
        indexRandom(LOCAL_CLUSTER, true, "data", 1);

        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        expectThrows(
            VerificationException.class,
            containsString("Unknown index [" + REMOTE_CLUSTER_1 + ":fake]"),
            () -> run(syncEsqlQueryRequest().query("FROM data," + REMOTE_CLUSTER_1 + ":fake"))
        );

        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        try (var response = run(syncEsqlQueryRequest().query("FROM data," + REMOTE_CLUSTER_1 + ":fake METADATA _index"))) {
            assertPartial(response);
            assertResultConcreteIndices(response, "data");
        }

        setSkipUnavailable(REMOTE_CLUSTER_1, null);
        try (var response = run(syncEsqlQueryRequest().query("FROM data," + REMOTE_CLUSTER_1 + ":fake METADATA _index"))) {
            assertPartial(response);
            assertResultConcreteIndices(response, "data");
        }
    }

    public void testResolvesLocalAndRemoteIndex() {
        indexRandom(LOCAL_CLUSTER, true, "index-1", 1);
        indexRandom(REMOTE_CLUSTER_1, true, "index-1", 1);

        try (var response = run(syncEsqlQueryRequest().query("FROM index-1," + REMOTE_CLUSTER_1 + ":index-1 METADATA _index"))) {
            assertOk(response);
            assertResultConcreteIndices(response, "index-1", REMOTE_CLUSTER_1 + ":index-1");
        }
    }

    public void testResolvesRemotesWithPattern() {
        indexRandom(LOCAL_CLUSTER, true, "index-1", 1);
        indexRandom(REMOTE_CLUSTER_1, true, "index-1", 1);
        indexRandom(REMOTE_CLUSTER_2, true, "index-1", 1);

        try (var response = run(syncEsqlQueryRequest().query("FROM *:index-1 METADATA _index"))) {
            assertOk(response);
            assertResultConcreteIndices(response, REMOTE_CLUSTER_1 + ":index-1", REMOTE_CLUSTER_2 + ":index-1"); // local is not included
        }
        try (var response = run(syncEsqlQueryRequest().query("FROM fake*:index-1 METADATA _index"))) {
            assertOk(response);
            assertResultConcreteIndices(response); // empty
        }
    }

    public void testDoesNotResolvesUnknownRemote() {
        expectThrows(
            NoSuchRemoteClusterException.class,
            containsString("no such remote cluster: [fake]"),
            () -> run(syncEsqlQueryRequest().query("FROM fake:index-1 METADATA _index"))
        );
    }

    private EsqlQueryResponse run(EsqlQueryRequest request) {
        try {
            return client(LOCAL_CLUSTER).execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    private void indexRandom(String clusterAlias, boolean forceRefresh, String index, int numDocs) {
        assert numDocs == 1;
        var client = client(clusterAlias);
        client.prepareIndex(index).setSource("f", "v").get();
        if (forceRefresh) {
            client.admin().indices().prepareRefresh(index).get();
        }
    }

    private static void assertOk(EsqlQueryResponse response) {
        assertThat(response.isPartial(), equalTo(false));
    }

    private static void assertPartial(EsqlQueryResponse response) {
        assertThat(response.isPartial(), equalTo(true));
    }

    private static void assertResultConcreteIndices(EsqlQueryResponse response, Object... indices) {
        var indexColumn = findIndexColumn(response);
        assertThat(() -> response.column(indexColumn), containsInAnyOrder(indices));
    }

    private static int findIndexColumn(EsqlQueryResponse response) {
        for (int c = 0; c < response.columns().size(); c++) {
            if (Objects.equals(response.columns().get(c).name(), MetadataAttribute.INDEX)) {
                return c;
            }
        }
        throw new AssertionError("no _index column found");
    }
}
