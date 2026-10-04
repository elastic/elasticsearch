/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Wide headerless CSV {@code COUNT(*)} under a small real request breaker. Declared mapping is required:
 * an inferred 105-column sample is hundreds of MiB and would trip 256 MB at planning. TEST-scoped so the
 * breaker limit does not leak into a SUITE-scoped sibling.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ExternalWideCsvCountStarIT extends AbstractExternalDataSourceIT {

    private static final int COLUMNS = 105;
    private static final int ROWS = 4_000;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "256mb")
            /*
             * Force standard settings for the request breaker or we may not break at all.
             * Without this we can randomly decide to use the {@code noop} breaker for request
             * and it won't break.....
             */
            .put(
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(),
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getDefault(Settings.EMPTY)
            )
            .put(
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(),
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getDefault(Settings.EMPTY)
            )
            .build();
    }

    public void testCountStarWideCsvManySplitsUnderSmallBreaker() throws Exception {
        Path file = createTempDir().resolve("wide.csv");
        writeHeaderlessWideCsv(file, ROWS, COLUMNS);
        long fileBytes = Files.size(file);
        assertThat("CSV minimumSegmentSize is 1 MiB; smaller files never form extra FileSplits", fileBytes, greaterThan(1024L * 1024));
        assertThat(
            "1kb target_split_size must produce more than 12 splits past the 1 MiB floor",
            (fileBytes - 1024L * 1024) / 1024,
            greaterThan(12L)
        );

        LinkedHashMap<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        for (int c = 0; c < COLUMNS; c++) {
            properties.put("col" + c, new DatasetFieldMapping("integer", null));
        }
        String dataset = registerStrictDataset(
            "wide_csv_count",
            StoragePath.fileUri(file),
            properties,
            Map.of("header_row", false, "target_split_size", "1kb")
        );
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*)"), TimeValue.timeValueMinutes(2))) {
            List<List<Object>> values = getValuesList(response);
            assertThat(values.size(), equalTo(1));
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo((long) ROWS));
        }
    }

    private static void writeHeaderlessWideCsv(Path file, int rows, int columns) throws Exception {
        StringBuilder sb = new StringBuilder(rows * columns * 4);
        for (int r = 0; r < rows; r++) {
            for (int c = 0; c < columns; c++) {
                if (c > 0) {
                    sb.append(',');
                }
                sb.append(c);
            }
            sb.append('\n');
        }
        Files.writeString(file, sb, StandardCharsets.UTF_8);
    }
}
