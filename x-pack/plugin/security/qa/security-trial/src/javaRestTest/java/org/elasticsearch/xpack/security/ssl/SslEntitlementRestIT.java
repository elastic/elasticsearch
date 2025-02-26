/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.ssl;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;
import org.junit.ClassRule;

public class SslEntitlementRestIT extends ESRestTestCase {

    private static MutableSettingsProvider settingsProvider = new MutableSettingsProvider();

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .apply(SecurityOnTrialLicenseRestTestCase.commonTrialSecurityClusterConfig)
        .settings(settingsProvider)
        // .setting("xpack.security.transport.ssl.enabled", "true")
        // .setting("xpack.security.transport.ssl.keystore.path", "/bad/path")
        .systemProperty("es.entitlements.enabled", "true")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testSslEntitlement() {
        settingsProvider.put("xpack.security.transport.ssl.keystore.path", "/bad/path");
        expectThrows(Exception.class, () -> cluster.restart(false));
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

}
