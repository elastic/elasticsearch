/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.action.AbstractExternalDataSourceIT;
import org.elasticsearch.xpack.esql.action.AbstractExternalDataSourceIT.EsqlEnterpriseWithDatasourceExtensions;
import org.elasticsearch.xpack.esql.action.EsqlPluginWithEnterpriseOrTrialLicense;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.telemetry.EsqlQueryMetricsCollector;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class EsqlQueryMetricsCollectorIT extends AbstractExternalDataSourceIT {

    /** Written by the SPI-instantiated collector; read by the test body. */
    static volatile Map<String, Long> lastMetrics;

    public static final class CapturingCollector implements EsqlQueryMetricsCollector {
        @Override
        public void onQueryCompleted(Map<String, Long> metrics) {
            EsqlQueryMetricsCollectorIT.lastMetrics = metrics;
        }
    }

    /**
     * Drops in for {@link EsqlEnterpriseWithDatasourceExtensions} and additionally loads
     * {@link EsqlQueryMetricsCollector} extensions by calling
     * {@link EsqlPlugin#loadMetricsCollectors}, which is suppressed in the base
     * {@link EsqlPluginWithEnterpriseOrTrialLicense} no-op override.
     */
    public static final class EsqlEnterpriseWithCollector extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            loadMetricsCollectors(loader);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(EsqlEnterpriseWithCollector.class);
        return plugins;
    }

    public void testCollectorCalledWithNonZeroMetrics() throws Exception {
        lastMetrics = null;

        Path dir = createTempDir();
        StringBuilder csv = new StringBuilder("emp_no:integer,name:keyword\n");
        for (int i = 0; i < 5; i++) {
            csv.append(i).append(",name_").append(i).append('\n');
        }
        Files.writeString(dir.resolve("data.csv"), csv.toString());

        String datasetName = registerDataset("metrics_test_ds", dir.resolve("data.csv").toUri().toString(), Map.of("format", "csv"));

        try (var ignored = run(syncEsqlQueryRequest("FROM " + datasetName + " | LIMIT 10"), TIMEOUT)) {
            // run the query — result discarded, only the collector side-effect matters
        }

        assertThat(lastMetrics, notNullValue());
        assertThat(lastMetrics.get(EsqlQueryMetricsCollector.PLANNING_NANOS), greaterThan(0L));
        assertThat(lastMetrics.get(EsqlQueryMetricsCollector.CPU_NANOS), greaterThan(0L));
        assertThat(lastMetrics.get(EsqlQueryMetricsCollector.READ_NANOS), greaterThan(0L));
    }
}
