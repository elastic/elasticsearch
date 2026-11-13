/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataSourceServiceEnableDisableTests extends ESTestCase {

    /**
     * A node that predates the flag reads every data source as enabled, so while one is in the cluster the toggle is
     * refused: the call must not report success for a disable that part of the cluster would ignore.
     * <p>
     * The cluster service and the encryption service are mocks because the refusal happens before anything but the cluster
     * state's minimum transport version is read, so neither is used; the state itself is a real one.
     */
    public void testToggleIsRefusedUntilAllNodesSupportTheFlag() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, Set.of(DataSourceService.MAX_DATA_SOURCES_COUNT_SETTING))
        );
        when(clusterService.state()).thenReturn(
            ClusterState.builder(ClusterName.DEFAULT)
                .putCompatibilityVersions(
                    "old-node",
                    TransportVersionUtils.randomVersionNotSupporting(DataSource.ESQL_DATASOURCE_DATASET_ENABLED),
                    Map.of()
                )
                .build()
        );
        DataSourceService service = new DataSourceService(clusterService, Map.of(), mock(EncryptionService.class));

        for (boolean enabled : new boolean[] { true, false }) {
            var future = new PlainActionFuture<AcknowledgedResponse>();
            service.setDataSourcesEnabled(
                ProjectId.DEFAULT,
                TimeValue.timeValueSeconds(30),
                TimeValue.timeValueSeconds(30),
                List.of("ds"),
                enabled,
                future
            );
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> future.actionGet(10, TimeUnit.SECONDS));
            assertThat(e.getMessage(), containsString("until all nodes in the cluster are upgraded"));
        }
    }
}
