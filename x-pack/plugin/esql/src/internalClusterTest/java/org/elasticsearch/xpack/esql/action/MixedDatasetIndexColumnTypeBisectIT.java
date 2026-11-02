/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

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

import org.apache.parquet.example.data.Group;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Bisects elastic/elasticsearch#164372 by COLUMN TYPE. The failure is a byte-misaligned read of an
 * {@code ExchangeResponse} page, detected inside a {@code BooleanBlock} read — but {@code readBoolean} is the only
 * block reader that validates its bytes, so boolean is the detector, not necessarily the culprit. Each test here
 * gives the dataset exactly ONE data column type and co-queries it with an index that shares no column, so the
 * union forces a null-filled branch. Whichever cell fails names the block type whose wire form disagrees.
 *
 * <p>{@code external_distribution=round_robin} forces the dataset splits onto non-coordinator nodes, so the pages
 * actually cross the wire instead of being handed over in-process.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 2)
public class MixedDatasetIndexColumnTypeBisectIT extends AbstractExternalDataSourceIT {

    private static final int FILES = 8;
    private static final int ROWS_PER_FILE = 25_000;
    private static final int DATASET_ROWS = FILES * ROWS_PER_FILE;
    private static final int INDEX_DOCS = 10;
    private static final String INDEX = "bisect_idx";

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
        request.pragmas(
            new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build())
        );
        request.acceptedPragmaRisks(true);
        try (EsqlQueryResponse response = run(request, TIMEOUT)) {
            return ((Number) getValuesList(response).get(0).get(0)).longValue();
        }
    }

    private void assertMixedCount(String dataset) {
        assertThat(mixedCount(dataset), equalTo((long) DATASET_ROWS + INDEX_DOCS));
    }

    public void testBooleanOnly() throws Exception {
        createDisjointIndex();
        assertMixedCount(registerSingleTypeDataset("ds_bool", "required boolean b;", (g, i) -> g.add("b", i % 2 == 0)));
    }

    public void testLongOnly() throws Exception {
        createDisjointIndex();
        assertMixedCount(registerSingleTypeDataset("ds_long", "required int64 l;", (g, i) -> g.add("l", (long) i)));
    }

    public void testIntOnly() throws Exception {
        createDisjointIndex();
        assertMixedCount(registerSingleTypeDataset("ds_int", "required int32 n;", (g, i) -> g.add("n", i)));
    }

    public void testDoubleOnly() throws Exception {
        createDisjointIndex();
        assertMixedCount(registerSingleTypeDataset("ds_double", "required double d;", (g, i) -> g.add("d", i * 1.5)));
    }

    /** Low cardinality: Parquet dictionary-encodes this, which can surface as an ordinals-backed BytesRef block. */
    public void testKeywordLowCardinalityOnly() throws Exception {
        createDisjointIndex();
        assertMixedCount(
            registerSingleTypeDataset("ds_kw_low", "required binary s (UTF8);", (g, i) -> g.add("s", "region_" + (i % 5)))
        );
    }

    /** High cardinality: a distinct value per row, so no dictionary is worth keeping. */
    public void testKeywordHighCardinalityOnly() throws Exception {
        createDisjointIndex();
        assertMixedCount(
            registerSingleTypeDataset("ds_kw_high", "required binary s (UTF8);", (g, i) -> g.add("s", "value_" + i))
        );
    }

    /**
     * The nullable numeric cells. The earlier matrix declared these {@code required}, which exercises the arm that
     * writes every slot and therefore proves nothing about the nullable path. These are the cells that decide whether
     * garbage in a nullable numeric column reaches the peer node without failing the query.
     */
    public void testNullableIntOnly() throws Exception {
        createDisjointIndex();
        assertMixedCount(registerSingleTypeDataset("ds_int_null", "optional int32 n;", (g, i) -> {
            if (i % 5 != 0) {
                g.add("n", i);
            }
        }));
    }

    public void testNullableLongOnly() throws Exception {
        createDisjointIndex();
        assertMixedCount(registerSingleTypeDataset("ds_long_null", "optional int64 l;", (g, i) -> {
            if (i % 5 != 0) {
                g.add("l", (long) i);
            }
        }));
    }

    public void testNullableDoubleOnly() throws Exception {
        createDisjointIndex();
        assertMixedCount(registerSingleTypeDataset("ds_double_null", "optional double d;", (g, i) -> {
            if (i % 5 != 0) {
                g.add("d", i * 1.5);
            }
        }));
    }

    /** A nullable low-cardinality string, so the dictionary path ships an ordinals block carrying garbage ordinals. */
    public void testNullableKeywordLowCardinalityOnly() throws Exception {
        createDisjointIndex();
        assertMixedCount(registerSingleTypeDataset("ds_kw_low_null", "optional binary s (UTF8);", (g, i) -> {
            if (i % 5 != 0) {
                g.add("s", "region_" + (i % 5));
            }
        }));
    }

    /** A nullable column, so the branch ships a null mask rather than a plain vector. */
    public void testNullableBooleanOnly() throws Exception {
        createDisjointIndex();
        assertMixedCount(registerSingleTypeDataset("ds_bool_null", "optional boolean b;", (g, i) -> {
            if (i % 5 != 0) {
                g.add("b", i % 4 == 0);
            }
        }));
    }
}
