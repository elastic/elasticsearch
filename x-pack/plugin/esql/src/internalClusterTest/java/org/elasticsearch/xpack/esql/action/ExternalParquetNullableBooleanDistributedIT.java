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

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Reproduces #164372: a nullable Parquet boolean column co-queried with an index must aggregate across both.
 *
 * <p>Masked positions of a nullable batch are left unassigned and serialized with the rest; deserializing a
 * boolean rejects any byte that is neither zero nor one. Two data nodes, because nothing reads a masked position
 * in-process. Whether any given run fails depends on what the recycled memory held, so the query is repeated:
 * with tens of thousands of masked positions per run the failure is usual, and repetition makes it reliable.
 * Per-type coverage is in the reader's unit tests, far more cheaply; this pins the distributed path.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 2)
public class ExternalParquetNullableBooleanDistributedIT extends AbstractExternalDataSourceIT {

    private static final int FILES = 8;
    private static final int ROWS_PER_FILE = 25_000;
    private static final int DATASET_ROWS = FILES * ROWS_PER_FILE;
    private static final int INDEX_DOCS = 10;
    private static final String INDEX = "disjoint_idx";
    /** Enough executions to make a data-dependent failure reliable. */
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
        assertAcked(indicesAdmin().prepareCreate(INDEX).setMapping("other_field", "type=keyword"));
        for (int i = 0; i < INDEX_DOCS; i++) {
            prepareIndex(INDEX).setSource("other_field", "v" + i).get();
        }
        refresh(INDEX);
    }

    /** Repeated because a single run's failure depends on what the reused memory held. */
    public void testNullableBooleanColumnAggregatesAcrossSources() throws Exception {
        createDisjointIndex();
        String dataset = registerSingleTypeDataset("ds_bool_null", "optional boolean b;", (g, i) -> {
            if (i % 5 != 0) {
                g.add("b", i % 4 == 0);
            }
        });
        for (int run = 0; run < RUNS; run++) {
            // The round_robin pragma is belt-and-braces: the default adaptive strategy already distributes this shape.
            EsqlQueryRequest request = syncEsqlQueryRequest("FROM " + dataset + ", " + INDEX + " | STATS c = COUNT(*)");
            request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build()));
            request.acceptedPragmaRisks(true);
            try (EsqlQueryResponse response = run(request, TIMEOUT)) {
                long count = ((Number) getValuesList(response).get(0).get(0)).longValue();
                assertThat(count, equalTo((long) DATASET_ROWS + INDEX_DOCS));
            }
        }
    }
}
