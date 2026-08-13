/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * End-to-end guard that declared-coercion {@code Warning} headers from the columnar Parquet reader stay bounded
 * across a query's whole fan-out. A read of a multi-file glob, each file spanning several row groups, splits into
 * many units spread over parallel drivers on each data node. Each unit builds its own capped collector, so without
 * a per-node ceiling the total {@code Warning} header count grows with the unit count until the response is
 * effectively undeliverable.
 *
 * <p>The fixture declares a {@code datetime} column over Parquet {@code keyword} values that are all unparseable,
 * so every value null-fills under {@code error_mode:null_field} and every unit wants to warn. The assertion is
 * that the coordinator's accumulated coercion warnings do not exceed {@code numDataNodes * (MAX_ADDED_WARNINGS + 1)}
 * (each node caps and deduplicates the whole informational channel through one shared budget), even though the
 * fixture presents far more distinct unparseable values than that ceiling.
 */
public class ExternalParquetCoercionWarningFloodIT extends AbstractExternalDataSourceIT {

    private static final int FILE_COUNT = 6;
    private static final int ROWS_PER_FILE = 200;
    /** Small row groups so each file spans several, multiplying the per-file unit count the budget must bound. */
    private static final int ROW_GROUP_BYTES = 4 * 1024;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    public void testDeclaredCoercionWarningsStayBoundedAcrossFanOut() throws Exception {
        Path dir = createTempDir();
        for (int f = 0; f < FILE_COUNT; f++) {
            final int fileOrdinal = f;
            writeParquet(
                dir.resolve("part-" + f + ".parquet"),
                "message ts_schema { required binary ts (UTF8); }",
                ROWS_PER_FILE,
                ROW_GROUP_BYTES,
                // Every token is distinct across the whole dataset and unparseable as a datetime, so a naive
                // per-unit cap would still admit many hundreds of distinct detail warnings.
                (g, i) -> g.add("ts", "notadate-" + fileOrdinal + "-" + i)
            );
        }

        registerDataSource("coercion_flood_ds", Map.of());
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", new DatasetFieldMapping("datetime", "ts"));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
        String datasetName = "coercion_flood";
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    datasetName,
                    "coercion_flood_ds",
                    globUri(dir, "*.parquet"),
                    null,
                    new HashMap<>(Map.of("error_mode", "null_field")),
                    mapping
                )
            )
        );

        // COUNT(ts) reads the ts column rather than answering purely from footer metadata; the present == 0
        // assertion below is the guarantee that a real scan-and-coerce happened (a metadata-only answer would
        // report the non-null row count instead of zero).
        String query = "FROM " + datasetName + " | STATS total = COUNT(*), present = COUNT(ts)";
        EsqlQueryRequest request = syncEsqlQueryRequest(query);

        DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        CountDownLatch latch = new CountDownLatch(1);
        List<String> warnings = new CopyOnWriteArrayList<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        AtomicReference<List<List<Object>>> values = new AtomicReference<>();
        client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
            try {
                values.set(getValuesList(response));
                ThreadContext threadContext = internalCluster().getInstance(TransportService.class, coordinator.getName())
                    .getThreadPool()
                    .getThreadContext();
                warnings.addAll(threadContext.getResponseHeaders().getOrDefault("Warning", List.of()));
            } finally {
                latch.countDown();
            }
        }, e -> {
            failure.set(e);
            latch.countDown();
        }));
        assertTrue("query did not complete within timeout", latch.await(2, TimeUnit.MINUTES));
        if (failure.get() != null) {
            throw new AssertionError("declared-coercion read must not fail, but did", failure.get());
        }

        long expectedTotal = (long) FILE_COUNT * ROWS_PER_FILE;
        assertThat("COUNT(*) must see every row", ((Number) values.get().get(0).get(0)).longValue(), equalTo(expectedTotal));
        assertThat(
            "every declared-datetime value is unparseable -> null-filled",
            ((Number) values.get().get(0).get(1)).longValue(),
            equalTo(0L)
        );

        List<String> coercionWarnings = warnings.stream().filter(w -> w.contains("coerce")).toList();
        assertThat(
            "the null-fill drift must still surface at least one coercion warning, got: " + warnings,
            coercionWarnings.size(),
            greaterThan(0)
        );
        // One shared budget per data node caps the whole informational channel (summaries, per-value details, and
        // the single overflow marker) at MAX_ADDED_WARNINGS + 1; the coordinator unions the per-node headers.
        int dataNodes = internalCluster().numDataNodes();
        long ceiling = (long) dataNodes * (SkipWarnings.MAX_ADDED_WARNINGS + 1);
        assertThat(
            "coercion Warning headers must stay within the per-node budget across the whole fan-out (dataNodes="
                + dataNodes
                + "), got "
                + coercionWarnings.size()
                + ": "
                + coercionWarnings,
            (long) coercionWarnings.size(),
            lessThanOrEqualTo(ceiling)
        );
    }

    private static String globUri(Path dir, String pattern) {
        String dirUri = StoragePath.fileUri(dir);
        if (dirUri.endsWith("/") == false) {
            dirUri += "/";
        }
        return dirUri + pattern;
    }
}
