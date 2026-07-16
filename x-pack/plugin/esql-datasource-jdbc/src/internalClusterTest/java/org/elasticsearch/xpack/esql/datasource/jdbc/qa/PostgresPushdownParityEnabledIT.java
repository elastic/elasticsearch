/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc.qa;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

/**
 * WHERE-pushdown parity suite with pushdown ENABLED (the default {@code esql.jdbc.pushdown.enabled=true}), against a
 * real {@code postgres:16.4} testcontainer. Every parity query must return the golden rows AND push its WHERE clause
 * into the connector's scan SQL. Its sibling {@link PostgresPushdownParityDisabledIT} runs the identical golden set
 * with pushdown OFF, so the two together establish on-vs-off result parity (see {@link AbstractJdbcPushdownParityIT}).
 */
@ThreadLeakFilters(filters = { PostgresTestThreadLeakFilter.class, HikariPoolTestThreadLeakFilter.class })
public class PostgresPushdownParityEnabledIT extends AbstractJdbcPushdownParityIT {

    @Override
    protected boolean pushdownEnabledForSuite() {
        return true;
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
