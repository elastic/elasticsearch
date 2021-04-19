/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsManager;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class DataFrameAnalyticsManagerTests extends ESTestCase {

    public void testNodeShuttingDown() {
        DataFrameAnalyticsManager manager =
            new DataFrameAnalyticsManager(
                Settings.EMPTY,
                mock(NodeClient.class),
                mock(ThreadPool.class),
                mock(ClusterService.class),
                mock(DataFrameAnalyticsConfigProvider.class),
                mock(AnalyticsProcessManager.class),
                mock(DataFrameAnalyticsAuditor.class),
                mock(IndexNameExpressionResolver.class),
                mock(ResultsPersisterService.class),
                mock(ModelLoadingService.class));
        assertThat(manager.isNodeShuttingDown(), is(false));

        manager.markNodeAsShuttingDown();
        assertThat(manager.isNodeShuttingDown(), is(true));
    }
}
