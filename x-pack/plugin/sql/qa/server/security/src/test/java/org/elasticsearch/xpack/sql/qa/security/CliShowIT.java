/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.security;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.sql.qa.cli.EmbeddedCli.SecurityConfig;
import org.elasticsearch.xpack.sql.qa.cli.ShowTestCase;

public class CliShowIT extends ShowTestCase {
    @Override
    protected Settings restClientSettings() {
        return RestSqlIT.securitySettings();
    }

    @Override
    protected String getProtocol() {
        return RestSqlIT.SSL_ENABLED ? "https" : "http";
    }

    @Override
    protected SecurityConfig securityConfig() {
        return CliSecurityIT.adminSecurityConfig();
    }
}
