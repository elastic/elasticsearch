/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.parquet.example.data.Group;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.ObjIntConsumer;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * A nullable Parquet boolean column, co-queried with an index, must aggregate across both sources.
 *
 * <p>The reader allocates each batch's value array uninitialized, on the contract that it writes every element.
 * A nullable arm writes only the positions carrying a value, so the masked positions keep prior memory and are
 * serialized with the rest. Deserializing a boolean rejects any byte that is neither zero nor one, so the query
 * fails outright. Two data nodes, because nothing reads a masked position in-process, and enough rows for the
 * write path to be compiled, because the byte is otherwise normalised before it reaches the stream.
 *
 * <p>The per-type coverage lives in the reader's own unit tests, which assert the invariant directly and far
 * more cheaply; this case exists to pin the distributed path.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 2)
public class ExternalParquetNullableBooleanDistributedIT extends AbstractExternalDataSourceIT {

    private static final int FILES = 8;
    private static final int ROWS_PER_FILE = 25_000;
    private static final int DATASET_ROWS = FILES * ROWS_PER_FILE;
    private static final int INDEX_DOCS = 10;
    private static final String INDEX = "disjoint_idx";
    /** Enough executions for the write path to be compiled; one cold run cannot fail. */
    private static final int RUNS = 10;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    private String registerSingleTypeDataset(String name, String column, ObjIntConsumer<Group> filler) throws IOException {
        Path dir = createTempDir().resolve(name);
        Files.createDirectories(dir);
        String schema = "message test { " + column + " }";
        for (int f = 0; f < FILES; f++) {
            int base = f * ROWS_PER_FILE;
            writeParquet(dir.resolve("part-" + f + ".parquet"), schema, ROWS_PER_FILE, 1024, (g, r) -> filler.accept(g, base + r));
        }
        String uri = StoragePath.fileUri(dir);
        if (uri.endsWith("/") == false) {
            uri += "/";
        }
        return registerDataset(name, uri + "*.parquet", Map.of());
    }

    private void createDisjointIndex() {
        assertTrue(indicesAdmin().prepareCreate(INDEX).setMapping("other_field", "type=keyword").get().isAcknowledged());
        for (int i = 0; i < INDEX_DOCS; i++) {
            prepareIndex(INDEX).setSource("other_field", "v" + i).get();
        }
        refresh(INDEX);
    }

    /**
     * Counts {@code FROM ds, idx | STATS COUNT(*)}. The {@code round_robin} pragma is belt-and-braces: for this
     * shape the default {@code adaptive} strategy already distributes, because the query reduces, the split count
     * exceeds the node count, and a mixed {@code FROM} has source siblings.
     */
    private long mixedCount(String dataset) {
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM " + dataset + ", " + INDEX + " | STATS c = COUNT(*)");
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build()));
        request.acceptedPragmaRisks(true);
        try (EsqlQueryResponse response = run(request, TIMEOUT)) {
            return ((Number) getValuesList(response).get(0).get(0)).longValue();
        }
    }

    private void assertMixedCount(String dataset) {
        assertThat(mixedCount(dataset), equalTo((long) DATASET_ROWS + INDEX_DOCS));
    }

    /** A nullable column, so the branch ships a null mask rather than a plain vector. */
    /**
     * The query is repeated because a single cold run cannot fail: the byte that breaks deserialization only
     * reaches the stream once the write path has been compiled, so one execution passes whether or not the
     * reader defines its masked positions.
     */
    public void testNullableBooleanColumnAggregatesAcrossSources() throws Exception {
        createDisjointIndex();
        String dataset = registerSingleTypeDataset("ds_bool_null", "optional boolean b;", (g, i) -> {
            if (i % 5 != 0) {
                g.add("b", i % 4 == 0);
            }
        });
        for (int run = 0; run < RUNS; run++) {
            assertMixedCount(dataset);
        }
    }
}
