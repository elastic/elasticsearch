/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc.qa;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

/**
 * WHERE-pushdown parity suite with pushdown DISABLED ({@code esql.jdbc.pushdown.enabled=false}), against a real
 * {@code postgres:16.4} testcontainer. The {@code JdbcConnectorFactory} reports no filter-pushdown support, so the
 * connector emits an unfiltered scan and the engine applies every filter. Each parity query must still return the
 * IDENTICAL golden rows as {@link PostgresPushdownParityEnabledIT} (that is the on-vs-off parity), and the connector's
 * scan SQL must carry NO {@code WHERE} clause — proving the kill switch genuinely turned pushdown off rather than the
 * two suites merely agreeing by coincidence. See {@link AbstractJdbcPushdownParityIT}.
 */
@ThreadLeakFilters(filters = { PostgresTestThreadLeakFilter.class, HikariPoolTestThreadLeakFilter.class })
public class PostgresPushdownParityDisabledIT extends AbstractJdbcPushdownParityIT {

    @Override
    protected boolean pushdownEnabledForSuite() {
        return false;
    }

    @Override
    protected JdbcDatabaseFixture createFixture() {
        return new PostgresFixture();
    }

    @Override
    protected boolean requiresDocker() {
        return true;
    }

    @Override
    protected boolean allowLoopback() {
        return true;
    }
}
