/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.qa.sql.security;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.qa.sql.jdbc.ResultSetTestCase;

import java.util.Properties;

/*
 * Integration testing class for "with security" (cluster running with the Security plugin,
 * and the Security is enabled) scenario. Runs all tests in the base class, the only changed
 * bit is the "environment".
 */
public class JdbcResultSetIT extends ResultSetTestCase {
    @Override
    protected Settings restClientSettings() {
        return RestSqlIT.securitySettings();
    }

    @Override
    protected String getProtocol() {
        return RestSqlIT.SSL_ENABLED ? "https" : "http";
    }

    @Override
    protected Properties connectionProperties() {
        Properties sp = super.connectionProperties();
        sp.putAll(JdbcSecurityIT.adminProperties());
        return sp;
    }
}
