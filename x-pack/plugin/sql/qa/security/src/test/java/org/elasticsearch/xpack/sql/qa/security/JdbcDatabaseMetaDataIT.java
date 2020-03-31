/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.security;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.sql.qa.jdbc.DatabaseMetaDataTestCase;

import java.util.Properties;

public class JdbcDatabaseMetaDataIT extends DatabaseMetaDataTestCase {
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
        Properties properties = super.connectionProperties();
        properties.putAll(JdbcSecurityIT.adminProperties());
        return properties;
    }
}
