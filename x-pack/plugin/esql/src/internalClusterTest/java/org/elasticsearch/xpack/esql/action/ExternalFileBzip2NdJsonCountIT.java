/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasource.bzip2.Bzip2DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests: {@code EXTERNAL} over HTTPS bzip2-compressed NDJSON (rally tracks), asserting
 * {@code STATS c = COUNT(*)}. The NYC taxis track is large; the suite timeout allows long scans.
 */
@TimeoutSuite(millis = 3 * TimeUnits.HOUR)
@ThreadLeakFilters(filters = ExternalFileBzip2NdJsonCountIT.JdkHttpClientThreadLeakFilter.class)
public class ExternalFileBzip2NdJsonCountIT extends AbstractEsqlIntegTestCase {

    private static final String REMOTE_LOOKUP_JOIN_BZIP2_NDJSON = "https://rally-tracks.elastic.co/joins/lookup_idx_100000_f10.json.bz2";

    private static final long EXPECTED_LOOKUP_JOIN_COUNT = 100_000L;

    private static final String REMOTE_NYC_TAXIS_BZIP2_NDJSON = "https://rally-tracks.elastic.co/nyc_taxis/documents.json.bz2";

    private static final long EXPECTED_NYC_TAXIS_COUNT = 165_346_692L;

    /**
     * {@link java.net.http.HttpClient} may leave a {@code HttpClient-*-SelectorManager} thread running briefly
     * after external reads; internal-cluster node shutdown does not always close the client before suite
     * teardown runs thread-leak checks.
     */
    public static final class JdkHttpClientThreadLeakFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            String name = t.getName();
            return name.startsWith("HttpClient-") && name.endsWith("SelectorManager");
        }
    }

    /**
     * {@link EsqlPluginWithEnterpriseOrTrialLicense} intentionally overrides {@link ExtensiblePlugin#loadExtensions}
     * with a no-op to avoid clashing with {@link org.elasticsearch.xpack.esql.plugin.EsqlPlugin}'s SPI path for some
     * extensions. For external data sources, {@link org.elasticsearch.plugins.MockPluginsService} only aggregates
     * {@link org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin} implementations when the ES|QL plugin
     * delegates to {@code super.loadExtensions(loader)}.
     */
    public static final class EsqlEnterpriseWithDatasourceExtensions extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            super.loadExtensions(loader);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(HttpDataSourcePlugin.class);
        plugins.add(Bzip2DataSourcePlugin.class);
        plugins.add(NdJsonDataSourcePlugin.class);
        return plugins;
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    public void testExternalBzip2NdJsonStatsCount() {
        assertExternalBzip2NdJsonCount(REMOTE_LOOKUP_JOIN_BZIP2_NDJSON, EXPECTED_LOOKUP_JOIN_COUNT, TimeValue.timeValueMinutes(10));
    }

    @AwaitsFix(bugUrl = "Very long-running test")
    public void testExternalBzip2NdJsonStatsCountNycTaxisDocuments() {
        assertExternalBzip2NdJsonCount(REMOTE_NYC_TAXIS_BZIP2_NDJSON, EXPECTED_NYC_TAXIS_COUNT, TimeValue.timeValueHours(3));
    }

    private void assertExternalBzip2NdJsonCount(String remoteUrl, long expectedCount, TimeValue requestTimeout) {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        String query = "EXTERNAL \"" + remoteUrl + "\" | STATS c = COUNT(*)";

        try (var response = run(syncEsqlQueryRequest(query), requestTimeout)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns.size(), equalTo(1));
            assertThat(columns.get(0).name(), equalTo("c"));

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            Object cell = rows.get(0).get(0);
            assertThat(((Number) cell).longValue(), equalTo(expectedCount));
        }
    }
}
