/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.operator.PageStreamPublisher;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.Before;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;

public class CrossClusterStreamQueryIT extends AbstractCrossClusterTestCase {

    private static final Pattern RANDOMIZED_RUNNER_SUFFIX_AT_END = Pattern.compile("(?:\\s+\\{[^}]*\\})+$");

    private String viewNameForTest(String prefix) {
        return prefix + RANDOMIZED_RUNNER_SUFFIX_AT_END.matcher(getTestName()).replaceFirst("").toLowerCase(Locale.ROOT);
    }

    @Before
    public void setupTwoClusters() throws Exception {
        setupClusters(2);
    }

    public void testDropNullColumnsKeepsFieldPopulatedOnlyOnRemote() throws Exception {
        client(REMOTE_CLUSTER_1).prepareIndex(REMOTE_INDEX).setSource("const", 42L).get();
        client(REMOTE_CLUSTER_1).admin().indices().prepareRefresh(REMOTE_INDEX).get();

        EsqlQueryRequest source = EsqlQueryRequest.syncEsqlQueryRequest(
            "FROM " + LOCAL_INDEX + "," + REMOTE_CLUSTER_1 + ":" + REMOTE_INDEX + " | KEEP const, v"
        );
        StreamQueryTestUtils.CountingStreamSubscriber subscriber = new StreamQueryTestUtils.CountingStreamSubscriber();
        EsqlStreamQueryAction.ResultStream resultStream = StreamQueryTestUtils.executeStreamRequest(
            client(LOCAL_CLUSTER),
            source,
            subscriber,
            true
        );

        assertThat("nullColumns must be set when dropNullColumns=true", resultStream.nullColumns(), notNullValue());
        int constIndex = indexOfColumn(resultStream, "const");
        assertNotEquals("const must be present in the output columns", -1, constIndex);
        assertFalse("const must not be marked null — it is populated on " + REMOTE_CLUSTER_1, resultStream.nullColumns()[constIndex]);
    }

    public void testDropNullColumnsDropsFieldEmptyOnAllClusters() throws Exception {

        EsqlQueryRequest source = EsqlQueryRequest.syncEsqlQueryRequest(
            "FROM " + LOCAL_INDEX + "," + REMOTE_CLUSTER_1 + ":" + REMOTE_INDEX + " | KEEP const, v"
        );
        StreamQueryTestUtils.CountingStreamSubscriber subscriber = new StreamQueryTestUtils.CountingStreamSubscriber();
        EsqlStreamQueryAction.ResultStream resultStream = StreamQueryTestUtils.executeStreamRequest(
            client(LOCAL_CLUSTER),
            source,
            subscriber,
            true
        );

        assertThat("nullColumns must be set when dropNullColumns=true", resultStream.nullColumns(), notNullValue());
        int constIndex = indexOfColumn(resultStream, "const");
        int vIndex = indexOfColumn(resultStream, "v");
        assertNotEquals("const must be present in the output columns", -1, constIndex);
        assertNotEquals("v must be present in the output columns", -1, vIndex);
        assertTrue("const must be marked null — it is empty on all clusters", resultStream.nullColumns()[constIndex]);
        assertFalse("v must not be marked null — it is populated on all clusters", resultStream.nullColumns()[vIndex]);
    }

    public void testDropNullColumnsViewOverRemoteIndexKeepsColumn() throws Exception {
        client(REMOTE_CLUSTER_1).prepareIndex(REMOTE_INDEX).setSource("const", 42L).get();
        client(REMOTE_CLUSTER_1).admin().indices().prepareRefresh(REMOTE_INDEX).get();

        String viewName = viewNameForTest("drop-null-ccs-view-");
        assertAcked(
            client(LOCAL_CLUSTER).execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(
                    TimeValue.THIRTY_SECONDS,
                    TimeValue.THIRTY_SECONDS,
                    new View(viewName, "FROM " + LOCAL_INDEX + "," + REMOTE_CLUSTER_1 + ":" + REMOTE_INDEX)
                )
            ).actionGet(30, TimeUnit.SECONDS)
        );

        EsqlQueryRequest source = EsqlQueryRequest.syncEsqlQueryRequest("FROM " + viewName + " | KEEP const, v");
        StreamQueryTestUtils.CountingStreamSubscriber subscriber = new StreamQueryTestUtils.CountingStreamSubscriber();
        EsqlStreamQueryAction.ResultStream resultStream = StreamQueryTestUtils.executeStreamRequest(
            client(LOCAL_CLUSTER),
            source,
            subscriber,
            true
        );

        assertThat("nullColumns must be set when dropNullColumns=true", resultStream.nullColumns(), notNullValue());
        int constIndex = indexOfColumn(resultStream, "const");
        assertNotEquals("const must be present in the output columns", -1, constIndex);
        assertFalse("const must not be marked null — it is populated on " + REMOTE_CLUSTER_1, resultStream.nullColumns()[constIndex]);
    }

    @SuppressWarnings("unchecked")
    public void testIncludeCcsMetadataInFooter() throws Exception {
        EsqlQueryRequest source = EsqlQueryRequest.syncEsqlQueryRequest(
            "FROM " + LOCAL_INDEX + "," + REMOTE_CLUSTER_1 + ":" + REMOTE_INDEX + " | STATS count(*)"
        );
        source.includeCCSMetadata(true);

        StreamQueryTestUtils.CountingStreamSubscriber subscriber = new StreamQueryTestUtils.CountingStreamSubscriber();
        EsqlStreamQueryAction.ResultStream resultStream = StreamQueryTestUtils.executeStreamRequest(
            client(LOCAL_CLUSTER),
            source,
            subscriber
        );

        PageStreamPublisher.StreamFooter footer = resultStream.publisher().footer();
        assertNotNull("footer must be present after the stream completes", footer);
        assertThat(footer.status(), equalTo(200));
        assertThat("clusters payload must be non-null with include_ccs_metadata=true", footer.clusters(), notNullValue());

        String clustersJson = Strings.toString((builder, params) -> {
            footer.clusters().toXContent(builder, params);
            return builder;
        });
        Map<String, Object> clusters;
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, clustersJson)) {
            clusters = parser.map();
        }

        assertThat("expected 2 clusters: local + one remote", clusters.get("total"), equalTo(2));
        assertThat("all clusters must succeed", clusters.get("successful"), equalTo(2));
        assertThat(clusters, hasKey("details"));
        Map<String, Object> details = (Map<String, Object>) clusters.get("details");
        assertThat("local cluster must appear in details", details, hasKey("(local)"));
        assertThat("remote cluster must appear in details", details, hasKey(REMOTE_CLUSTER_1));
        Map<String, Object> localDetails = (Map<String, Object>) details.get("(local)");
        assertThat(localDetails, hasKey("shards"));
        Map<String, Object> remoteDetails = (Map<String, Object>) details.get(REMOTE_CLUSTER_1);
        assertThat(remoteDetails, hasKey("shards"));
    }

    private static int indexOfColumn(EsqlStreamQueryAction.ResultStream resultStream, String name) {
        for (int i = 0; i < resultStream.columns().size(); i++) {
            if (resultStream.columns().get(i).name().equals(name)) {
                return i;
            }
        }
        return -1;
    }
}
