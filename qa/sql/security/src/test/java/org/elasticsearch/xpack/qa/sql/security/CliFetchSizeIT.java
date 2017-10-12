/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.security;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.qa.sql.cli.FetchSizeTestCase;

public class CliFetchSizeIT extends FetchSizeTestCase {
    static String securityEsUrlPrefix() {
        return "test_admin:x-pack-test-password@";
    }
    @Override
    protected Settings restClientSettings() {
        return RestSqlIT.securitySettings();
    }

    @Override
    protected String esUrlPrefix() {
        return securityEsUrlPrefix();
    }
}
