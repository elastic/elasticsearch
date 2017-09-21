/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.security;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.qa.sql.jdbc.ConnectionTestCase;

import java.util.Properties;

public class JdbcConnectionIT extends ConnectionTestCase {
    static Properties securityProperties() {
        Properties prop = new Properties();
        prop.put("user", "test_admin");
        prop.put("pass", "x-pack-test-password");
        return prop;
    }

    @Override
    protected Settings restClientSettings() {
        return RestSqlIT.securitySettings();
    }

    @Override
    protected Properties connectionProperties() {
        return securityProperties();
    }
}
