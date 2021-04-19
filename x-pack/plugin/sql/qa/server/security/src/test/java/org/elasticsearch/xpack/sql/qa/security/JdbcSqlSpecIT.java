/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.security;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.sql.qa.jdbc.SqlSpecTestCase;

import java.util.Properties;

public class JdbcSqlSpecIT extends SqlSpecTestCase {
    public JdbcSqlSpecIT(String fileName, String groupName, String testName, Integer lineNumber, String query) {
        super(fileName, groupName, testName, lineNumber, query);
    }

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
