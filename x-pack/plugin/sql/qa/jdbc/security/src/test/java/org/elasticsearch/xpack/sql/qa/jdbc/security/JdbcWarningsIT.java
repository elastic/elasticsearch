/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.jdbc.security;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.sql.qa.jdbc.JdbcWarningsTestCase;

import java.util.Properties;

public class JdbcWarningsIT extends JdbcWarningsTestCase {

    @Override
    protected Settings restClientSettings() {
        return JdbcConnectionIT.securitySettings();
    }

    @Override
    protected String getProtocol() {
        return JdbcConnectionIT.SSL_ENABLED ? "https" : "http";
    }

    @Override
    protected Properties connectionProperties() {
        Properties properties = super.connectionProperties();
        properties.putAll(JdbcSecurityUtils.adminProperties());
        return properties;
    }

}
