/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetReaderStatus;
import org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceOperator;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Row-group pruning for the out-of-band DSL request filter over a Parquet dataset — the Kibana time-picker case.
 *
 * <p>Partition pruning skips whole files and is covered by {@code ExternalHivePartitionPruningIT}. A time filter cannot
 * reach it: a partition column is path-derived and never typed as a date, so a range on an ordinary timestamp column
 * prunes nothing at the file layer. What it can do is skip row groups inside the file on their statistics, and that is
 * what these tests assert, by reading {@code row_groups_kept} against {@code row_groups_total} from the source
 * operator's {@code format_reader} status.
 *
 * <p>Every query drains fully — no {@code LIMIT} — because the producer commits its format-reader snapshot on the
 * terminal EOF callback, and a short-circuited scan can leave the consumer reading an empty status.
 */
public class ExternalRequestFilterRowGroupPruningIT extends AbstractExternalDataSourceIT {

    private static final int ROWS = 500;
    /** Row {@code i} carries {@code ts = i * 10}, so the column is monotonic and its row groups are disjoint in time. */
    private static final int TS_STEP = 10;
    private static final String[] REGIONS = { "US", "EU", "AP" };

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    public void testRequestFilterTimeRangeSkipsRowGroups() throws Exception {
        String dataset = registerSortedDataset("rg_range");
        // ts in [0, 100] selects rows 0..10 out of 500 — a slice narrow enough that most row groups cannot hold a match.
        Pruning status = runAndReadStatus(dataset, QueryBuilders.rangeQuery("ts").gte(0).lte(100), idsWithTsBetween(0, 100));
        assertTrue("the range must be pushed to the reader", status.pushdownUsed());
        assertThat("the fixture must have more than one row group or there is nothing to skip", status.total(), greaterThan(1L));
        assertThat("a selective time range must skip row groups", status.kept(), lessThan(status.total()));
    }

    public void testWhereAndRequestFilterPruneIdentically() throws Exception {
        String dataset = registerSortedDataset("rg_parity");
        List<Long> expected = idsWithTsBetween(0, 100);
        Pruning viaFilter = runAndReadStatus(dataset, QueryBuilders.rangeQuery("ts").gte(0).lte(100), expected);
        // The bounds are cast: ts is LONG and a bare ES|QL integer literal is INTEGER, which mv_in_range rejects.
        Pruning viaWhere = runAndReadStatus(dataset, "WHERE mv_in_range(ts, 0::long, 100::long) | ", null, expected);
        assertThat("the request filter and the equivalent WHERE must skip the same row groups", viaFilter.kept(), equalTo(viaWhere.kept()));
    }

    public void testUnfilteredScanKeepsEveryRowGroup() throws Exception {
        // The control the pruning assertions are measured against: without a filter nothing may be skipped.
        String dataset = registerSortedDataset("rg_control");
        Pruning status = runAndReadStatus(dataset, null, allIds());
        assertThat(status.kept(), equalTo(status.total()));
    }

    public void testMustNotKeepsEveryRowGroup() throws Exception {
        // Under a negation the multivalue forms are not pushed at all: the pruning bound is a superset, and a superset
        // under NOT is an under-match that no retained filter can undo, because the rows were never read. So the scan
        // must be total while the answer stays exact.
        String dataset = registerSortedDataset("rg_mustnot");
        List<Long> expected = new ArrayList<>();
        for (long id = 0; id < ROWS; id++) {
            if (REGIONS[(int) (id % REGIONS.length)].equals("EU") == false) {
                expected.add(id);
            }
        }
        Pruning status = runAndReadStatus(dataset, QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("region", "EU")), expected);
        assertThat("a negated filter must not prune row groups", status.kept(), equalTo(status.total()));
    }

    public void testCaseInsensitiveTermKeepsEveryRowGroup() throws Exception {
        // A case_insensitive term becomes mv_contains(TO_LOWER(region), "eu"). Statistics hold the original case, so
        // pruning against the lowered literal would under-match. The rows must still be exactly the EU rows.
        String dataset = registerSortedDataset("rg_ci");
        List<Long> expected = new ArrayList<>();
        for (long id = 0; id < ROWS; id++) {
            if (REGIONS[(int) (id % REGIONS.length)].equals("EU")) {
                expected.add(id);
            }
        }
        Pruning status = runAndReadStatus(dataset, QueryBuilders.termQuery("region", "eu").caseInsensitive(true), expected);
        assertThat("a case-insensitive term must not prune row groups", status.kept(), equalTo(status.total()));
    }

    public void testTimeRangeAndedWithATermStillSkipsRowGroups() throws Exception {
        // The shape that matters in practice: a time range plus a filter pill. The term arm cannot prune on this
        // fixture (every row group holds all three regions), so the range arm must carry the pruning.
        String dataset = registerSortedDataset("rg_and");
        List<Long> expected = new ArrayList<>();
        for (long id : idsWithTsBetween(0, 1000)) {
            if (REGIONS[(int) (id % REGIONS.length)].equals("EU")) {
                expected.add(id);
            }
        }
        Pruning status = runAndReadStatus(
            dataset,
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.rangeQuery("ts").gte(0).lte(1000))
                .filter(QueryBuilders.termQuery("region", "EU")),
            expected
        );
        assertThat("the range arm must still skip row groups", status.kept(), lessThan(status.total()));
    }

    /** Row-group accounting for one query, summed over every external source operator that ran. */
    private record Pruning(long kept, long total, boolean pushdownUsed) {}

    private Pruning runAndReadStatus(String dataset, QueryBuilder requestFilter, List<Long> expectedIds) {
        return runAndReadStatus(dataset, "", requestFilter, expectedIds);
    }

    /**
     * Runs {@code FROM <dataset> | <prefix>KEEP id | SORT id ASC}, asserts the surviving ids are exactly
     * {@code expectedIds} — computed from the fixture's definition, not from what the engine returns — and gives back
     * the row-group accounting so the caller can assert what was skipped.
     *
     * <p>The counters are summed across every external source operator in the profile rather than read off one of
     * them. A scan can be split over several drivers, each reporting only the row groups it saw, so picking one makes
     * the numbers depend on driver order — two runs of the same query then disagree without anything being wrong.
     */
    private Pruning runAndReadStatus(String dataset, String prefix, QueryBuilder requestFilter, List<Long> expectedIds) {
        var request = syncEsqlQueryRequest("FROM " + dataset + " | " + prefix + "KEEP id | SORT id ASC");
        request.profile(true);
        if (requestFilter != null) {
            request.filter(requestFilter);
        }
        try (var response = run(request)) {
            List<Long> actual = getValuesList(response).stream().map(row -> ((Number) row.get(0)).longValue()).toList();
            assertThat("the filter must return exactly the matching rows", actual, equalTo(expectedIds));

            assertThat(response.profile(), notNullValue());
            long kept = 0;
            long total = 0;
            boolean pushdownUsed = false;
            int readers = 0;
            for (var driver : response.profile().drivers()) {
                for (var op : driver.operators()) {
                    if (op.status() instanceof AsyncExternalSourceOperator.Status s
                        && s.formatReader() instanceof ParquetReaderStatus parquet) {
                        kept += parquet.rowGroupsKept();
                        total += parquet.rowGroupsTotal();
                        pushdownUsed |= parquet.predicatePushdownUsed();
                        readers++;
                    }
                }
            }
            assertThat("expected a Parquet reader status in the driver profiles", readers, greaterThan(0));
            return new Pruning(kept, total, pushdownUsed);
        }
    }

    private List<Long> idsWithTsBetween(long lower, long upper) {
        List<Long> ids = new ArrayList<>();
        for (long id = 0; id < ROWS; id++) {
            long ts = id * TS_STEP;
            if (ts >= lower && ts <= upper) {
                ids.add(id);
            }
        }
        return ids;
    }

    private List<Long> allIds() {
        return idsWithTsBetween(Long.MIN_VALUE, Long.MAX_VALUE);
    }

    /**
     * One Parquet file of {@link #ROWS} rows ordered by {@code ts}, written with a small row-group size so the file
     * holds many row groups whose {@code ts} statistics are disjoint — the layout a time filter can actually prune.
     */
    private String registerSortedDataset(String name) throws IOException {
        Path file = createTempDir().resolve(name + ".parquet");
        writeParquet(
            file,
            "message test { required int64 id; required int64 ts; required binary region (UTF8); }",
            ROWS,
            256,
            (group, i) -> {
                group.add("id", (long) i);
                group.add("ts", (long) i * TS_STEP);
                group.add("region", REGIONS[i % REGIONS.length]);
            }
        );
        return registerDataset(name, StoragePath.fileUri(file), Map.of());
    }
}
