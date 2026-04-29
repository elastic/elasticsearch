/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.jdbc.security.without_ssl;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.sql.qa.jdbc.JdbcErrorsTestCase;
import org.elasticsearch.xpack.sql.qa.jdbc.JdbcSecurityUtils;
import org.junit.ClassRule;

import java.util.Properties;

public class SecurityWithoutSslJdbcJdbcErrorsIT extends JdbcErrorsTestCase {
    @ClassRule
    public static final ElasticsearchCluster cluster = withSecurityCluster(false);

    @Override
    public ElasticsearchCluster getCluster() {
        return cluster;
    }

    @Override
    protected Settings restClientSettings() {
        return securitySettings(false);
    }

    @Override
    protected Properties connectionProperties() {
        Properties properties = super.connectionProperties();
        properties.putAll(JdbcSecurityUtils.adminProperties(false));
        return properties;
    }
}
