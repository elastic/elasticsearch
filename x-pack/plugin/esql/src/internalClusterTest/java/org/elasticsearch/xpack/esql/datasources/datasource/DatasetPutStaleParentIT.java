/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.xpack.esql.datasources.dataset.GetDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Registering a dataset must not depend on which node receives the request.
 *
 * <p>A client that creates a data source and then creates a dataset against it issues two calls with
 * nothing in between. The first is a success status, so the second is entitled to see the parent.
 * When the two land on different nodes and the second node has not yet applied the publication that
 * carries the parent, the request must still be registered: the master re-validates authoritatively
 * and holds the only state that can decide.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class DatasetPutStaleParentIT extends ESIntegTestCase {

    /** Short, so the blocked node's missing ack is not waited out for the full default. */
    private static final TimeValue SHORT_ACK = TimeValue.timeValueSeconds(1);
    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestEncryptionServicePlugin.class, DataSourceCrudIT.LocalStateDataSource.class);
    }

    public void testDatasetRegistersOnANodeThatHasNotYetAppliedTheParent() throws Exception {
        final String master = internalCluster().getMasterName();
        final String lagging = randomValueOtherThan(master, () -> randomFrom(internalCluster().getNodeNames()));

        // The lagging node stops applying published cluster state, so it cannot see anything created from now on.
        final BlockClusterStateProcessing blocked = new BlockClusterStateProcessing(lagging, random());
        internalCluster().setDisruptionScheme(blocked);
        blocked.startDisrupting();
        try {
            // Created on the master. The lagging node cannot ack, so this answers acknowledged=false —
            // a success status that a client reasonably reads as "created".
            AcknowledgedResponse created = client(master).execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(TIMEOUT, SHORT_ACK, "cb", "test", null, new HashMap<>())
            ).actionGet(TIMEOUT);
            assertThat("the blocked node cannot ack", created.isAcknowledged(), equalTo(false));

            // The same client now registers the dataset, and it reaches the node that is behind.
            client(lagging).execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(TIMEOUT, SHORT_ACK, "hits", "cb", "test://hits", null, new HashMap<>())
            ).actionGet(TIMEOUT);

            // GET is a local read: ask the master, which holds the committed metadata, while the
            // lagging node is still blocked.
            GetDatasetAction.Request get = new GetDatasetAction.Request(TIMEOUT);
            get.indices("hits");
            GetDatasetAction.Response got = client(master).execute(GetDatasetAction.INSTANCE, get).actionGet(TIMEOUT);
            assertThat(got.getDatasets(), hasSize(1));
            Dataset dataset = got.getDatasets().iterator().next();
            assertThat(dataset.name(), equalTo("hits"));
            assertThat(dataset.dataSource().getName(), equalTo("cb"));
            assertThat(dataset.resource(), equalTo("test://hits"));
        } finally {
            blocked.stopDisrupting();
            internalCluster().clearDisruptionScheme();
        }
    }
}
