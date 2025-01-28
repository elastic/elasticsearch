/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.monitoring.exporter.local;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.monitoring.cleaner.CleanerService;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringMigrationCoordinator;

import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class LocalExporterTests extends ESTestCase {

    public void testLocalExporterRemovesListenersOnClose() {
        final ClusterService clusterService = mock(ClusterService.class);
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        final Exporter.Config config = new Exporter.Config("name", "type", Settings.EMPTY, clusterService, licenseState);
        final CleanerService cleanerService = mock(CleanerService.class);
        final MonitoringMigrationCoordinator migrationCoordinator = new MonitoringMigrationCoordinator();
        final LocalExporter exporter = new LocalExporter(config, mock(Client.class), migrationCoordinator, cleanerService);
        verify(clusterService).addListener(exporter);
        verify(cleanerService).add(exporter);
        verify(licenseState).addListener(exporter);
        exporter.close();
        verify(clusterService).removeListener(exporter);
        verify(cleanerService).remove(exporter);
        verify(licenseState).removeListener(exporter);
    }

    public void testLocalExporterDoesNotInteractWithClusterServiceUntilStateIsRecovered() {
        final ClusterService clusterService = mock(ClusterService.class);
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        final Exporter.Config config = new Exporter.Config("name", "type", Settings.EMPTY, clusterService, licenseState);
        final CleanerService cleanerService = mock(CleanerService.class);
        final MonitoringMigrationCoordinator migrationCoordinator = new MonitoringMigrationCoordinator();
        try (var threadPool = createThreadPool()) {
            final var client = new NoOpClient(threadPool);
            final LocalExporter exporter = new LocalExporter(config, client, migrationCoordinator, cleanerService);

            final TimeValue retention = TimeValue.timeValueDays(randomIntBetween(1, 90));
            exporter.onCleanUpIndices(retention);

            verify(clusterService).addListener(same(exporter));
            verifyNoMoreInteractions(clusterService);

            final ClusterState oldState = ClusterState.EMPTY_STATE;
            final ClusterState newState = ClusterStateCreationUtils.stateWithNoShard();
            exporter.clusterChanged(new ClusterChangedEvent(getTestName(), newState, oldState));
            verify(clusterService).localNode();

            exporter.onCleanUpIndices(retention);
            verify(clusterService).state();
            verify(clusterService, times(2)).localNode();
        }
    }
}
