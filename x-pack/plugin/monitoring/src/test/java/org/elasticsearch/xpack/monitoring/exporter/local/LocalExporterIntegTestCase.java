/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.local;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.monitoring.cleaner.CleanerService;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringMigrationCoordinator;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * {@code LocalExporterIntegTestCase} offers a basis for integration tests for the {@link LocalExporter}.
 */
public abstract class LocalExporterIntegTestCase extends MonitoringIntegTestCase {

    protected final String exporterName = "_local";

    private static ThreadPool THREADPOOL;

    @BeforeClass
    public static void setupThreadPool() {
        THREADPOOL = new TestThreadPool(LocalExporterIntegTestCase.class.getName());
    }

    @AfterClass
    public static void cleanUpStatic() {
        if (THREADPOOL != null) {
            terminate(THREADPOOL);
        }
    }

    protected Settings localExporterSettings() {
        return Settings.builder()
            .put("xpack.monitoring.collection.enabled", false)
            .put("xpack.monitoring.collection.interval", "1s")
            .put("xpack.monitoring.exporters." + exporterName + ".type", LocalExporter.TYPE)
            .put("xpack.monitoring.exporters." + exporterName + ".enabled", false)
            .put("xpack.monitoring.exporters." + exporterName + ".cluster_alerts.management.enabled", false)
            .put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false)
            .put(XPackSettings.PROFILING_ENABLED.getKey(), false)
            .put(XPackSettings.APM_ENABLED.getKey(), false)
            .build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).put(localExporterSettings()).build();
    }

    /**
     * Create a new {@link LocalExporter} with the default exporter settings and name.
     *
     * @return Never {@code null}.
     */
    protected LocalExporter createLocalExporter() {
        return createLocalExporter(exporterName, null, new MonitoringMigrationCoordinator());
    }

    /**
     * Create a new {@link LocalExporter}. Expected usage:
     * <pre><code>
     * final Settings settings = Settings.builder().put("xpack.monitoring.exporters._local.type", "local").build();
     * try (LocalExporter exporter = createLocalExporter("_local", settings)) {
     *   // ...
     * }
     * </code></pre>
     *
     * @return Never {@code null}.
     */
    protected LocalExporter createLocalExporter(String localExporterName, Settings exporterSettings) {
        return createLocalExporter(localExporterName, exporterSettings, new MonitoringMigrationCoordinator());
    }

    protected LocalExporter createLocalExporter(
        String localExporterName,
        Settings exporterSettings,
        MonitoringMigrationCoordinator coordinator
    ) {
        final XPackLicenseState licenseState = TestUtils.newTestLicenseState();
        final Exporter.Config config = new Exporter.Config(localExporterName, "local", exporterSettings, clusterService(), licenseState);
        final CleanerService cleanerService = new CleanerService(exporterSettings, clusterService().getClusterSettings(), THREADPOOL);
        return new LocalExporter(config, client(), coordinator, cleanerService);
    }

}
