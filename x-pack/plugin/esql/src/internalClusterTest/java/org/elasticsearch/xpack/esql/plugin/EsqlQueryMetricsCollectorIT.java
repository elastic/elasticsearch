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
import org.elasticsearch.xpack.core.esql.QueryMetricsListener;
import org.elasticsearch.xpack.esql.action.AbstractExternalDataSourceIT;
import org.elasticsearch.xpack.esql.action.EsqlPluginWithEnterpriseOrTrialLicense;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class EsqlQueryMetricsCollectorIT extends AbstractExternalDataSourceIT {

    /** Written by the SPI-instantiated collector; read by the test body. */
    static volatile Map<String, Long> lastMetrics;

    public static final class CapturingCollector implements QueryMetricsListener {
        @Override
        public void onQueryCompleted(Map<String, Long> metrics) {
            EsqlQueryMetricsCollectorIT.lastMetrics = metrics;
        }
    }

    /**
     * Drops in for {@link EsqlEnterpriseWithDatasourceExtensions} and additionally loads
     * {@link QueryMetricsListener} extensions by calling
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

    public void testMetricsCollector() throws Exception {
        // We're using local data source here, which needs the flag
        assumeTrue("requires local filesystem feature flag", HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled());
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
        assertThat(lastMetrics.get(QueryMetricsListener.PLANNING_NANOS), greaterThan(0L));
        assertThat(lastMetrics.get(QueryMetricsListener.CPU_NANOS), greaterThan(0L));
        assertThat(lastMetrics.get(QueryMetricsListener.READ_NANOS), greaterThan(0L));
        assertThat(lastMetrics.get(QueryMetricsListener.READ_CPU_NANOS), greaterThan(0L));
        // CPU counter should not exceed total counter
        assertThat(
            lastMetrics.get(QueryMetricsListener.READ_NANOS),
            greaterThanOrEqualTo(lastMetrics.get(QueryMetricsListener.READ_CPU_NANOS))
        );
        assertThat(lastMetrics.get(QueryMetricsListener.SPLIT_DISCOVERY_NANOS), greaterThan(0L));
        // TODO: does not work for CVS for now: assertThat(lastMetrics.get(QueryMetricsListener.BYTES_READ), greaterThan(0L));
    }

    public void testNoCollectionWithoutExternalData() throws Exception {
        // We're using local data source here, which needs the flag
        assumeTrue("requires local filesystem feature flag", HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled());
        lastMetrics = null;

        try (var ignored = run(syncEsqlQueryRequest("ROW a=1 | LIMIT 10"), TIMEOUT)) {
            // run the query — result discarded, only the collector side-effect matters
        }

        assertThat(lastMetrics, nullValue());

        createIndex("test");
        try (var ignored = run(syncEsqlQueryRequest("FROM test | LIMIT 10"), TIMEOUT)) {
            // run the query — result discarded, only the collector side-effect matters
        }

        assertThat(lastMetrics, nullValue());
    }

    public void testWarmAggregate() throws Exception {
        assumeTrue("requires local filesystem feature flag", HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled());

        Path dir = createTempDir();
        StringBuilder csv = new StringBuilder("emp_no:integer,salary:long\n");
        for (int i = 0; i < 5; i++) {
            csv.append(i).append(",").append(50000 + i).append('\n');
        }
        Files.writeString(dir.resolve("data.csv"), csv.toString());

        String ds = registerDataset("warm_agg_ds", dir.resolve("data.csv").toUri().toString(), Map.of("format", "csv"));

        // Cold scan: populates the stats cache (row count, column stats).
        lastMetrics = null;
        try (var ignored = run(syncEsqlQueryRequest("FROM " + ds + " | LIMIT 200"), TIMEOUT)) {}
        assertThat("cold scan must be metered", lastMetrics, notNullValue());

        // Warm aggregate: answered from cached stats — splitsScanned stays 0,
        // so the gate in hasExternalSources() does not fire even though the
        // external datasource drove the result.
        lastMetrics = null;
        try (var ignored = run(syncEsqlQueryRequest("FROM " + ds + " | STATS COUNT(*)"), TIMEOUT)) {}
        assertThat("warm COUNT(*) over external source must be metered", lastMetrics, notNullValue());
    }
}
