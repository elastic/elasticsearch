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
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Characterizes the narrow-projection aggregate over a multi-file CSV glob whose shared column the
 * coordinator widened to KEYWORD under {@code union_by_name} (cross-file type disagreement). This is the
 * query shape that crashes at the EC2 226-CSV-file / 105-column scale with
 * {@code SchemaAdaptingIterator}'s "output schema size [1] does not match mapping width [105]" IAE — but
 * it does NOT crash in this single-coordinator, {@code file://} setup, and this test documents why.
 * <p>
 * Two files declare {@code col} with disagreeing inferred types — integer in {@code a.csv}, non-numeric
 * string in {@code b.csv}. Under {@code schema_resolution = union_by_name} the coordinator reconciles
 * {@code col} to KEYWORD ({@code SchemaReconciliation#reconcileUnionByName}), producing a per-file
 * {@link org.elasticsearch.xpack.esql.datasources.ColumnMapping} with a KEYWORD cast slot at unified
 * width. {@code STATS MIN(col)} is a single-column projection.
 * <p>
 * <b>Why this passes locally.</b> The width-vs-projection guard in
 * {@code SchemaAdaptingIterator}'s constructor relies on
 * {@code FileSplitProvider#discoverSplits} having narrowed each split's {@code ColumnMapping} to the query
 * projection via {@code ColumnMapping#pruneToPerFileQuery}. That narrowing is gated on
 * {@code context.unifiedSchema() != null} (FileSplitProvider: "when null — legacy callers, data-node
 * paths — the per-file mapping stays at Unified width"). In a single-coordinator {@code file://} flow the
 * {@code ExternalSourceExec} is always built fresh via {@code ExternalRelation#toPhysicalExec}, which
 * unconditionally seeds a non-null {@code unifiedSchema}, so split discovery prunes the mapping to the
 * projected width and the guard's invariant holds. (Verified by instrumentation: the per-file mapping
 * width 3 prunes to width 1, matching the size-1 {@code queryDataSchema}.) The crash requires the mapping
 * to reach the iterator UN-pruned at unified width while the projection is narrowed — which needs
 * {@code unifiedSchema} to be null at the node that discovers/attaches splits, a condition this local
 * topology cannot construct because {@code unifiedSchema} is not serialized
 * ({@code ExternalSourceExec#writeTo} omits it) and is only reachable on a remote/distributed boundary.
 * <p>
 * The IAE itself is a pre-existing main-branch latent bug introduced by PRs #149088 and #149930 (both
 * predate this PR's merge-base); this branch's stripe-stats changes touch none of the crash-path classes.
 * This test is the positive-path coverage proving the narrow-projection-under-UBN path is correct in the
 * single-coordinator case.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3)
public class ExternalCsvUnionByNameNarrowProjectionIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    /**
     * Force splits onto data nodes (round_robin) and the parallel-parse path (parsing_parallelism > 1) so
     * the read goes through {@code AsyncExternalSourceOperatorFactory#adaptSchema}, mirroring the EC2
     * 1rg-per-file shape. Even under distribution the coordinator prunes the mapping before shipping, so
     * the width matches the projection.
     */
    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(Settings.builder().put("external_distribution", "round_robin").put("parsing_parallelism", 4).build());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).put("esql.source.cache.stripe.size", "64kb").build();
    }

    public void testUnionByNameNarrowMinProjection() throws Exception {
        Path dir = createTempDir().resolve("ubn_narrow_min");
        Files.createDirectories(dir);
        // col: integer in a.csv, non-numeric string in b.csv -> reconciles to KEYWORD under union_by_name.
        Files.writeString(dir.resolve("a.csv"), "id,col,note\n1,123,alpha\n2,456,gamma\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("b.csv"), "id,col,note\n4,abc,beta\n5,def,epsilon\n", StandardCharsets.UTF_8);

        String glob = StoragePath.fileUri(dir) + "/*.csv";
        String dataset = registerDataset("ubn_narrow", glob, Map.of("schema_resolution", "union_by_name"));
        String query = "FROM " + dataset + " | STATS lo = MIN(col)";

        try (var response = run(syncEsqlQueryRequest(query))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            // MIN over a KEYWORD column = lexicographic minimum across all files: "123" < "456" < "abc" < "def".
            assertThat(rows.get(0).get(0), equalTo("123"));
        }
    }
}
