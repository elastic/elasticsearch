/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.ssl;

import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.is;

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

    public void testSslEntitlementInaccessiblePath() throws IOException {
        settingsProvider.put("xpack.security.transport.ssl.key", "/bad/path/transport.key");
        settingsProvider.put("xpack.security.transport.ssl.certificate", "/bad/path/transport.crt");
        expectThrows(Exception.class, () -> cluster.restart(false));
        AtomicBoolean found = new AtomicBoolean(false);
        for (int i = 0; i < cluster.getNumNodes(); i++) {
            try (InputStream log = cluster.getNodeLog(i, LogType.SERVER)) {
                Streams.readAllLines(log, line -> {
                    if (line.contains("failed to load SSL configuration") && line.contains("because access to read the file is blocked")) {
                        found.set(true);
                    }
                });
            }
        }
        assertThat("cluster logs did not include events of blocked file access", found.get(), is(true));
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
