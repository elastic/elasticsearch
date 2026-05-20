/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Regression test for NDJSON record-aligned macro splits when parsing_parallelism=1.
 * The query forces many macro splits and validates exact row accounting across split boundaries.
 */
public class ExternalNdJsonRecordAlignedFallbackIT extends AbstractEsqlIntegTestCase {

    /**
     * Re-enables extension loading that {@link EsqlPluginWithEnterpriseOrTrialLicense} suppresses.
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
        plugins.add(NdJsonDataSourcePlugin.class);
        return plugins;
    }

    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 1).build());
    }

    public void testCountMinMaxWithMacroSplitsInSingleThreadFallback() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        int rows = 20_000;
        Path ndjsonFile = writeNdjsonFile(rows);
        try {
            String query = "EXTERNAL \""
                + StoragePath.fileUri(ndjsonFile)
                + "\" WITH {\"target_split_size\":\"1kb\"} | STATS c = COUNT(*), mn = MIN(a), mx = MAX(a)";

            var request = syncEsqlQueryRequest(query);
            request.profile(true);

            try (var response = run(request, TimeValue.timeValueMinutes(5))) {
                List<? extends ColumnInfo> columns = response.columns();
                assertThat(columns.size(), equalTo(3));
                assertThat(columns.get(0).name(), equalTo("c"));
                assertThat(columns.get(1).name(), equalTo("mn"));
                assertThat(columns.get(2).name(), equalTo("mx"));

                List<List<Object>> values = getValuesList(response);
                assertThat(values.size(), equalTo(1));
                List<Object> row = values.get(0);
                assertThat(((Number) row.get(0)).longValue(), equalTo((long) rows));
                assertThat(((Number) row.get(1)).longValue(), equalTo(0L));
                assertThat(((Number) row.get(2)).longValue(), equalTo((long) rows - 1));

                long asyncOps = response.profile()
                    .drivers()
                    .stream()
                    .flatMap(driver -> driver.operators().stream())
                    .filter(op -> op.operator().startsWith("AsyncExternalSourceOperator"))
                    .count();
                assertThat(asyncOps, greaterThanOrEqualTo(1L));
            }
        } finally {
            Files.deleteIfExists(ndjsonFile);
        }
    }

    private Path writeNdjsonFile(int rows) throws Exception {
        Path file = createTempDir().resolve("macro-splits.ndjson");
        StringBuilder sb = new StringBuilder(rows * 16);
        for (int i = 0; i < rows; i++) {
            sb.append("{\"a\":").append(i).append("}\n");
        }
        Files.writeString(file, sb);
        return file;
    }
}
