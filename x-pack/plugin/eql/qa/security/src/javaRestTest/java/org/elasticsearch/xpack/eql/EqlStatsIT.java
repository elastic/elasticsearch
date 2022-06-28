/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.eql.stats.EqlUsageRestTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.eql.SecurityUtils.secureClientSettings;

public class EqlStatsIT extends EqlUsageRestTestCase {

    /**
     * All tests run as a superuser but use <code>es-security-runas-user</code> to become a less privileged user.
     */
    @Override
    protected Settings restClientSettings() {
        return secureClientSettings();
    }

    @Override
    protected void runRequest(Request request) throws IOException {
        SecurityUtils.setRunAsHeader(request, "test-admin");
        super.runRequest(request);
    }
}
