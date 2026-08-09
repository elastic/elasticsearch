/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc.qa;

import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.datasource.jdbc.qa.JdbcTestQuerySet.Fixture;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

/**
 * Runs the shared {@link JdbcTestQuerySet} correctness matrix against in-process H2 — the baseline that proves the
 * QA harness works end-to-end (parser → dataset rewriter → external source resolver → JDBC connector → H2 → back)
 * without any Docker-backed database. Needs no Docker and no external process: {@link
 * H2Fixture} keeps the whole database in the test JVM.
 * <p>
 * Almost all behaviour lives in {@link AbstractJdbcDatabaseIT}; this subclass supplies the H2 fixture (so the exact
 * same assertions a real-database suite runs are exercised here first) plus a value-bearing temporal read
 * ({@link #testTemporalValueReadAnchorsToUtc()}) that locks the {@link
 * org.elasticsearch.xpack.esql.datasource.jdbc.ColumnReader} DATETIME extraction on H2 in the same way
 * {@code PostgresJdbcIT} does on Postgres.
 */
public class H2JdbcIT extends AbstractJdbcDatabaseIT {

    @Override
    protected JdbcDatabaseFixture createFixture() {
        return new H2Fixture();
    }

    /**
     * The in-process H2 TCP server listens on {@code localhost}, so the dataset URL's host is loopback; enable
     * loopback so the {@code SsrfGuard} accepts it.
     */
    @Override
    protected boolean allowLoopback() {
        return true;
    }

    /**
     * The H2 fixture uses a {@code jdbc:h2:tcp://} URL (the only H2 form ESQL's resolver can parse), which the
     * production SSRF default allowlist excludes; permit it for this suite.
     */
    @Override
    protected List<String> allowedSubprotocols() {
        return List.of("jdbc:h2:tcp://");
    }

    /**
     * Value-bearing temporal read against H2. The shared matrix only asserts the temporal <em>type</em> on an EMPTY
     * result ({@code types_temporal_columns_empty}, {@code WHERE id == -999}), so it does not materialize an H2 temporal
     * VALUE end-to-end. This drives a NON-empty projection of the {@code types_matrix} id-1 row
     * ({@code date_val DATE '2020-01-01'}, {@code ts_val TIMESTAMP '2020-01-01 08:00:00'}) so
     * {@link org.elasticsearch.xpack.esql.datasource.jdbc.ColumnReader} runs, and asserts the exact rendered instants.
     * <p>
     * H2's {@code DATE}/{@code TIMESTAMP} are naive (no time zone); the reader extracts them via
     * {@code rs.getTimestamp(col, <Calendar in UTC>).toInstant()}, anchoring each wall clock to UTC. That makes the
     * expected values independent of the (randomized) JVM default time zone: {@code date_val} → midnight UTC, and
     * {@code ts_val} → {@code 08:00} UTC. ES|QL renders {@code DATETIME} with the default
     * {@code strict_date_optional_time} formatter (millisecond precision), so the cells are {@code 2020-01-01T00:00:00.000Z}
    * and {@code 2020-01-01T08:00:00.000Z}. This is the H2 side of the temporal-read parity: both
    * H2 and Postgres materialize temporal values through the one UTC-anchored path.
     */
    public void testTemporalValueReadAnchorsToUtc() {
        String dataset = datasetNameFor(Fixture.TYPES_MATRIX);
        try (EsqlQueryResponse response = run("FROM " + dataset + " | WHERE id == 1 | KEEP date_val, ts_val", queryTimeout())) {
            List<ColumnInfo> columns = new ArrayList<>(response.columns());
            assertThat(columns.stream().map(ColumnInfo::name).toList(), containsInAnyOrder("date_val", "ts_val"));
            for (ColumnInfo column : columns) {
                assertEquals("column [" + column.name() + "] must be a DATETIME", "date", column.outputType());
            }
            List<List<Object>> rows = getValuesList(response);
            assertThat("expected exactly one row for id == 1", rows.size(), equalTo(1));
            assertThat(rows.get(0), contains("2020-01-01T00:00:00.000Z", "2020-01-01T08:00:00.000Z"));
        }
    }
}
