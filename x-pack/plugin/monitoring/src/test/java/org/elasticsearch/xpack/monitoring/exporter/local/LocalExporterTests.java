/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.monitoring.exporter.local;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.monitoring.cleaner.CleanerService;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class LocalExporterTests extends ESTestCase {

    public void testLocalExporterRemovesListenersOnClose() {
        final ClusterService clusterService = mock(ClusterService.class);
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        final Exporter.Config config = new Exporter.Config("name", "type", Settings.EMPTY, clusterService, licenseState);
        final CleanerService cleanerService = mock(CleanerService.class);
        final LocalExporter exporter = new LocalExporter(config, mock(Client.class), cleanerService);
        verify(clusterService).addListener(exporter);
        verify(cleanerService).add(exporter);
        verify(licenseState).addListener(exporter);
        exporter.close();
        verify(clusterService).removeListener(exporter);
        verify(cleanerService).remove(exporter);
        verify(licenseState).removeListener(exporter);
    }

}
