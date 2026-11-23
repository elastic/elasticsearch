/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * A multi-line CSV dataset read under a lenient {@code error_mode} must resume at the first record after an oversized
 * one that spans a line, and must report only the oversized record as malformed.
 */
public class ExternalCsvOversizedRecordRecoveryIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    public void testRowAfterOversizedMultiLineRecordIsRead() throws Exception {
        assumeTrue("external_max_record_size pragma is snapshot-only", Build.current().isSnapshot());
        Path file = createTempDir().resolve("oversized.csv");
        Files.writeString(
            file,
            "id,note,tag\n1,short,a\n2,\"a note that runs past the record size cap, and then a newline\n\",b\n3,\"x\",c\n4,y,d\n",
            StandardCharsets.UTF_8
        );
        String dataset = registerDataset(
            "oversized_multiline",
            StoragePath.fileUri(file),
            Map.of("error_mode", "skip_row", "max_errors", 100, "multiline_fields", true)
        );
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM " + dataset + " | KEEP id | SORT id").pragmas(
            new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_RECORD_SIZE.getKey(), "32b").build())
        );

        DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        CountDownLatch latch = new CountDownLatch(1);
        List<Object> ids = new CopyOnWriteArrayList<>();
        List<String> warnings = new CopyOnWriteArrayList<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
            try {
                getValuesList(response).forEach(row -> ids.add(row.get(0)));
                warnings.addAll(
                    internalCluster().getInstance(TransportService.class, coordinator.getName())
                        .getThreadPool()
                        .getThreadContext()
                        .getResponseHeaders()
                        .getOrDefault("Warning", List.of())
                );
            } finally {
                latch.countDown();
            }
        }, e -> {
            failure.set(e);
            latch.countDown();
        }));
        assertTrue("query did not complete within 2 minutes", latch.await(2, TimeUnit.MINUTES));
        if (failure.get() != null) {
            throw new AssertionError("lenient read must not fail", failure.get());
        }
        assertThat(ids.stream().map(id -> ((Number) id).intValue()).toList(), equalTo(List.of(1, 3, 4)));
        assertTrue("expected the row [2] warning, got: " + warnings, warnings.stream().anyMatch(w -> w.contains("row [2]")));
        assertTrue(
            "row [3] is well formed and must not be reported, got: " + warnings,
            warnings.stream().noneMatch(w -> w.contains("row [3]") || w.contains("unexpected content after a closing quote"))
        );
        assertThat(warnings.stream().filter(w -> w.contains("record exceeds")).count(), equalTo(1L));
        assertTrue(warnings.stream().anyMatch(w -> w.contains("record exceeds [32b]")));
    }
}
