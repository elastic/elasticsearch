/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * End-to-end guard for the partition-filter wrong-answer, fixed alongside the #153503 hive partition-identity work:
 * {@code FROM <hive-partitioned parquet> | WHERE <partition col> LIKE …} must return only the matching
 * partitions, on a distributed read.
 *
 * <p>Root cause it locks: on a data node the coordinator {@code FileList} is {@code UNRESOLVED}, so the
 * old {@code PushFiltersToSource} partition-name lookup (which read the fileList) returned empty and let
 * the partition conjunct be minted into the parquet reader predicate. Parquet classifies the LIKE-family
 * ({@code LIKE}, {@code STARTS_WITH}) as fully-pushed ({@code Pushability.YES}) and drops the
 * {@code FilterExec}, so the predicate — over a column absent from the file payload — never filters and
 * every partition's rows survive (5 where 3 are correct). Equality/range are {@code RECHECK} and were
 * corrected by the retained {@code FilterExec}, which is why only the LIKE-family showed the bug. The fix
 * reads partition names from the serialized stamp (node-safe) so the conjunct is held in the
 * {@code FilterExec} on every node.
 *
 * <p>Forced distributed ({@code round_robin}, ≥2 data nodes) on purpose: a coordinator-local run has a
 * resolved fileList and would false-green the whole class.
 */
public class ExternalParquetHivePartitionFilterPushdownIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    private String twoPartitionDataset(String name) throws Exception {
        Path root = createTempDir().resolve(name);
        writeSingleColumnIdParquet(root.resolve("p=a"), 3); // ids 0,1,2
        writeSingleColumnIdParquet(root.resolve("p=b"), 2); // ids 0,1
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.parquet";
        return registerDataset(name, glob, Map.of("hive_partitioning", true));
    }

    private List<List<Object>> runDistributed(String query) {
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build());
        var request = syncEsqlQueryRequest(query);
        request.pragmas(pragmas);
        request.acceptedPragmaRisks(true); // pragmas are rejected on non-snapshot builds without this
        request.profile(true);
        try (var response = run(request)) {
            assertThat(
                "external scan must distribute across >= 2 data nodes to exercise the UNRESOLVED-FileList path",
                externalScanNodeNames(response).size(),
                greaterThanOrEqualTo(2)
            );
            return getValuesList(response);
        }
    }

    /** LIKE on a partition column: the regression cell — YES-pushed, dropped from FilterExec, all rows leaked pre-fix. */
    public void testWhereLikePartitionReturnsOnlyMatchingPartition() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String dataset = twoPartitionDataset("hive_where_like");
        List<List<Object>> rows = runDistributed("FROM " + dataset + " | WHERE p LIKE \"a*\"");
        assertThat("WHERE p LIKE a* must return only the 3 rows under p=a", rows.size(), equalTo(3));
    }

    /** STARTS_WITH is also YES-pushed — same leak family. */
    public void testWhereStartsWithPartitionReturnsOnlyMatchingPartition() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String dataset = twoPartitionDataset("hive_where_startswith");
        List<List<Object>> rows = runDistributed("FROM " + dataset + " | WHERE STARTS_WITH(p, \"a\")");
        assertThat("WHERE STARTS_WITH(p, a) must return only the 3 rows under p=a", rows.size(), equalTo(3));
    }

    /** Equality was RECHECK (correct even pre-fix); pin it so the fix doesn't regress the working path. */
    public void testWhereEqPartitionReturnsOnlyMatchingPartition() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String dataset = twoPartitionDataset("hive_where_eq");
        List<List<Object>> rows = runDistributed("FROM " + dataset + " | WHERE p == \"a\"");
        assertThat("WHERE p == a must return only the 3 rows under p=a", rows.size(), equalTo(3));
    }

    /** Mixed partition (LIKE) + data conjunct: the partition half must be held, the data half may push. */
    public void testWhereMixedPartitionAndDataConjunct() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String dataset = twoPartitionDataset("hive_where_mixed");
        List<List<Object>> rows = runDistributed("FROM " + dataset + " | WHERE p LIKE \"a*\" AND id >= 1");
        // p=a holds ids 0,1,2; id >= 1 keeps 1,2 → 2 rows.
        assertThat("mixed partition-LIKE + data filter must apply both", rows.size(), equalTo(2));
    }

    /** Prune-all boundary: a partition predicate matching nothing must return zero rows, not all of them. */
    public void testWherePartitionPrunesAll() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String dataset = twoPartitionDataset("hive_where_prune_all");
        List<List<Object>> rows = runDistributed("FROM " + dataset + " | WHERE p == \"zz\"");
        assertThat("a partition filter matching no partition must return no rows", rows.size(), equalTo(0));
    }

    /** Ordered comparison directly on the partition column (a RECHECK op): p > "a" keeps only p=b. */
    public void testWhereRangeOnPartitionColumn() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String dataset = twoPartitionDataset("hive_where_range");
        List<List<Object>> rows = runDistributed("FROM " + dataset + " | WHERE p > \"a\"");
        assertThat("p > a must keep only the two p=b rows", rows.size(), equalTo(2));
    }
}
