/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamFailureStore;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractCrossClusterTestCase;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo.Cluster.Status;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RemoteIndexResolutionIT extends AbstractCrossClusterTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return CollectionUtils.concatLists(List.of(MapperExtrasPlugin.class, DataStreamsPlugin.class), super.nodePlugins(clusterAlias));
    }

    public void testResolvesRemoteIndex() {
        indexRandom(REMOTE_CLUSTER_1, true, "index-1", 1);

        try (var response = run(syncEsqlQueryRequest("FROM " + REMOTE_CLUSTER_1 + ":index-1 METADATA _index").includeCCSMetadata(true))) {
            assertOk(response);
            assertResultConcreteIndices(response, REMOTE_CLUSTER_1 + ":index-1");
            assertExecutionInfo(response, new EsqlResponseExecutionInfo(REMOTE_CLUSTER_1, "index-1", Status.SUCCESSFUL));
        }
    }

    public void testResolveRemoteUnknownIndex() {
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        expectThrows(
            VerificationException.class,
            containsString("Unknown index [" + REMOTE_CLUSTER_1 + ":fake]"),
            () -> run(syncEsqlQueryRequest("FROM " + REMOTE_CLUSTER_1 + ":fake"))
        );
        expectThrows(
            VerificationException.class,
            containsString("Unknown index [" + REMOTE_CLUSTER_1 + ":fake]"),
            () -> run(syncEsqlQueryRequest("FROM " + REMOTE_CLUSTER_1 + ":fake").allowPartialResults(true))
        );

        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        try (var response = run(syncEsqlQueryRequest("FROM " + REMOTE_CLUSTER_1 + ":fake METADATA _index").includeCCSMetadata(true))) {
            assertPartial(response);
            assertResultConcreteIndices(response);
            assertExecutionInfo(response, new EsqlResponseExecutionInfo(REMOTE_CLUSTER_1, "fake", Status.SKIPPED));
        }

        setSkipUnavailable(REMOTE_CLUSTER_1, null);
        try (var response = run(syncEsqlQueryRequest("FROM " + REMOTE_CLUSTER_1 + ":fake METADATA _index").includeCCSMetadata(true))) {
            assertPartial(response);
            assertResultConcreteIndices(response);
            assertExecutionInfo(response, new EsqlResponseExecutionInfo(REMOTE_CLUSTER_1, "fake", Status.SKIPPED));
        }
    }

    public void testResolvesLocalAndRemoteIndex() {
        indexRandom(LOCAL_CLUSTER, true, "index-1", 1);
        indexRandom(REMOTE_CLUSTER_1, true, "index-1", 1);

        try (
            var response = run(
                syncEsqlQueryRequest("FROM index-1," + REMOTE_CLUSTER_1 + ":index-1 METADATA _index").includeCCSMetadata(true)
            )
        ) {
            assertOk(response);
            assertResultConcreteIndices(response, "index-1", REMOTE_CLUSTER_1 + ":index-1");
            assertExecutionInfo(
                response,
                new EsqlResponseExecutionInfo(LOCAL_CLUSTER, "index-1", Status.SUCCESSFUL),
                new EsqlResponseExecutionInfo(REMOTE_CLUSTER_1, "index-1", Status.SUCCESSFUL)
            );
        }
    }

    public void testResolvesRemotesWithPattern() {
        indexRandom(LOCAL_CLUSTER, true, "index-1", 1);
        indexRandom(REMOTE_CLUSTER_1, true, "index-1", 1);
        indexRandom(REMOTE_CLUSTER_2, true, "index-1", 1);

        try (var response = run(syncEsqlQueryRequest("FROM *:index-1 METADATA _index").includeCCSMetadata(true))) {
            assertOk(response);
            assertResultConcreteIndices(response, REMOTE_CLUSTER_1 + ":index-1", REMOTE_CLUSTER_2 + ":index-1"); // local is not included
            assertExecutionInfo(
                response,
                new EsqlResponseExecutionInfo(REMOTE_CLUSTER_1, "index-1", Status.SUCCESSFUL),
                new EsqlResponseExecutionInfo(REMOTE_CLUSTER_2, "index-1", Status.SUCCESSFUL)
            );
        }
        try (var response = run(syncEsqlQueryRequest("FROM fake*:index-1 METADATA _index").includeCCSMetadata(true))) {
            assertOk(response);
            assertResultConcreteIndices(response); // empty
            assertExecutionInfo(response); // empty
        }
    }

    public void testDoesNotResolvesUnknownRemote() {
        expectThrows(
            NoSuchRemoteClusterException.class,
            containsString("no such remote cluster: [fake]"),
            () -> run(syncEsqlQueryRequest("FROM fake:index-1 METADATA _index"))
        );
    }

    public void testResolutionWithFilter() {
        indexRandom(REMOTE_CLUSTER_1, true, "index-1", 1);

        try (
            var response = run(
                syncEsqlQueryRequest("FROM " + REMOTE_CLUSTER_1 + ":index-1 METADATA _index").filter(new MatchAllQueryBuilder())
            )
        ) {
            assertOk(response);
            assertResultConcreteIndices(response, REMOTE_CLUSTER_1 + ":index-1");
            assertExecutionInfo(response, new EsqlResponseExecutionInfo(REMOTE_CLUSTER_1, "index-1", Status.SUCCESSFUL));
        }
        try (
            var response = run(
                syncEsqlQueryRequest("FROM " + REMOTE_CLUSTER_1 + ":index-1 METADATA _index").filter(new MatchNoneQueryBuilder())
            )
        ) {
            assertOk(response);
            assertResultConcreteIndices(response);
            assertExecutionInfo(response, new EsqlResponseExecutionInfo(REMOTE_CLUSTER_1, "index-1", Status.SUCCESSFUL));
        }
    }

    public void testResolvesDataStream() {
        assertAcked(
            client(REMOTE_CLUSTER_1).execute(
                TransportPutComposableIndexTemplateAction.TYPE,
                new TransportPutComposableIndexTemplateAction.Request("data-stream-1-template").indexTemplate(
                    ComposableIndexTemplate.builder()
                        .indexPatterns(List.of("data-stream-1*"))
                        .template(
                            Template.builder()
                                .dataStreamOptions(new DataStreamOptions.Template(new DataStreamFailureStore.Template(true, null)))
                        )
                        .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                        .build()
                )
            )
        );
        assertAcked(
            client(REMOTE_CLUSTER_1).execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "data-stream-1")
            )
        );

        client(REMOTE_CLUSTER_1).bulk(
            new BulkRequest() // create a valid and invalid document to initialize failure store
                .add(client(REMOTE_CLUSTER_1).prepareIndex("data-stream-1").setOpType(OpType.CREATE).setSource("@timestamp", 1).request())
                .add(client(REMOTE_CLUSTER_1).prepareIndex("data-stream-1").setOpType(OpType.CREATE).setSource("@timestamp", "i").request())
        ).actionGet();

        try (var response = run(syncEsqlQueryRequest("FROM " + REMOTE_CLUSTER_1 + ":data-stream-1"))) {
            assertOk(response);
        }
        try (var response = run(syncEsqlQueryRequest("FROM " + REMOTE_CLUSTER_1 + ":data-stream-1::data"))) {
            assertOk(response);
        }
        try (var response = run(syncEsqlQueryRequest("FROM " + REMOTE_CLUSTER_1 + ":data-stream-1::failures"))) {
            assertOk(response);
        }
        expectThrows(
            org.elasticsearch.xpack.esql.parser.ParsingException.class,
            containsString("Invalid index name [data-stream-1::fake]"),
            () -> run(syncEsqlQueryRequest("FROM " + REMOTE_CLUSTER_1 + ":data-stream-1::fake"))
        );

        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        expectThrows(
            VerificationException.class,
            containsString("Unknown index [" + REMOTE_CLUSTER_1 + ":no-such-data-stream::data]"),
            () -> run(syncEsqlQueryRequest("FROM " + REMOTE_CLUSTER_1 + ":no-such-data-stream::data"))
        );

        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        try (var response = run(syncEsqlQueryRequest("FROM " + REMOTE_CLUSTER_1 + ":no-such-data-stream::data"))) {
            assertPartial(response);
            assertExecutionInfo(response, new EsqlResponseExecutionInfo(REMOTE_CLUSTER_1, "no-such-data-stream::data", Status.SKIPPED));
        }

        setSkipUnavailable(REMOTE_CLUSTER_1, null);
        try (var response = run(syncEsqlQueryRequest("FROM " + REMOTE_CLUSTER_1 + ":no-such-data-stream::data"))) {
            assertPartial(response);
            assertExecutionInfo(response, new EsqlResponseExecutionInfo(REMOTE_CLUSTER_1, "no-such-data-stream::data", Status.SKIPPED));
        }
    }

    private EsqlQueryResponse run(EsqlQueryRequest request) {
        return client(LOCAL_CLUSTER).execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS);
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

    private static void assertExecutionInfo(EsqlQueryResponse response, EsqlResponseExecutionInfo... infos) {
        assertThat(executionInfo(response), containsInAnyOrder(infos));
    }

    private static int findIndexColumn(EsqlQueryResponse response) {
        for (int c = 0; c < response.columns().size(); c++) {
            if (Objects.equals(response.columns().get(c).name(), MetadataAttribute.INDEX)) {
                return c;
            }
        }
        throw new AssertionError("no _index column found");
    }

    private static List<EsqlResponseExecutionInfo> executionInfo(EsqlQueryResponse response) {
        return response.getExecutionInfo()
            .getClusters()
            .values()
            .stream()
            .map(cluster -> new EsqlResponseExecutionInfo(cluster.getClusterAlias(), cluster.getIndexExpression(), cluster.getStatus()))
            .toList();
    }

    private record EsqlResponseExecutionInfo(String alias, String index, Status status) {}
}
