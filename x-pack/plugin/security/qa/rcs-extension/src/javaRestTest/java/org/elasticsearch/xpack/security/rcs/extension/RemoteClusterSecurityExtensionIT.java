/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rcs.extension;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import static org.hamcrest.Matchers.equalTo;

public class RemoteClusterSecurityExtensionIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .name("test-rcs-extension-cluster")
        .plugin("test-rcs-extension-plugin")
        .setting("xpack.security.enabled", "true")
        .user("test-admin", "x-pack-test-password")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
            .put(
                ThreadContext.PREFIX + ".Authorization",
                basicAuthHeaderValue("test-admin", new SecureString("x-pack-test-password".toCharArray()))
            )
            .build();
    }

    /**
     * Simple test which checks if rcs extension is loaded
     * by asserting that RCS extension's setting is returned
     * by {@code /_cluster/settings} API.
     */
    public void testExtensionIsLoaded() throws Exception {
        var settings = assertOKAndCreateObjectPath(client().performRequest(new Request("GET", "/_cluster/settings?include_defaults=true")));
        assertThat(settings.evaluate("defaults.xpack.test.rcs.extension.setting"), equalTo("default-rcs-extension-setting-value"));
    }
}
