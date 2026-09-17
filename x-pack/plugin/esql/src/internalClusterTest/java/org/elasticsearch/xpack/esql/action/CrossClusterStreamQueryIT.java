/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.notNullValue;

public class CrossClusterStreamQueryIT extends AbstractCrossClusterTestCase {

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

        String viewName = "drop-null-ccs-view-" + getTestName().toLowerCase(java.util.Locale.ROOT);
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

    private static int indexOfColumn(EsqlStreamQueryAction.ResultStream resultStream, String name) {
        for (int i = 0; i < resultStream.columns().size(); i++) {
            if (resultStream.columns().get(i).name().equals(name)) {
                return i;
            }
        }
        return -1;
    }
}
