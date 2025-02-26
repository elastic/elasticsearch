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

    private static final MutableSettingsProvider settingsProvider = new MutableSettingsProvider();

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .apply(SecurityOnTrialLicenseRestTestCase.commonTrialSecurityClusterConfig)
        .settings(settingsProvider)
        .systemProperty("es.entitlements.enabled", "true")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testSslEntitlementInaccessiblePath() {
        settingsProvider.put("xpack.security.transport.ssl.keystore.path", "/bad/path");
        expectThrows(Exception.class, () -> cluster.restart(false));
        // TODO assert on cluster logs
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

    @Override
    protected boolean preserveClusterUponCompletion() {
        // as the cluster is dead its state can not be wiped successfully so we have to bypass wiping the cluster
        return true;
    }
}
