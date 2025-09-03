/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.system.indices;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

public abstract class AbstractSystemIndicesIT extends ESRestTestCase {
    protected static final String BASIC_AUTH_VALUE = basicAuthHeaderValue(
        "rest_user",
        new SecureString("rest-user-password".toCharArray())
    );

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .plugin("system-indices-qa")
        .module("ingest-common")
        .module("x-pack-migrate")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .user("rest_user", "rest-user-password")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE).build();
    }
}
