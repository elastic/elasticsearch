/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.encryption.spi.test;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Verifies that {@code EncryptedDataHandlerProvider} implementations contributed by other plugins via
 * {@code ExtensiblePlugin.loadExtensions} are discovered by the encryption plugin and their handlers are invoked by the running
 * {@code KeyRotationCoordinator}.
 *
 * <p>The test plugin contributes a {@code TestEncryptedDataHandlerProvider} whose handler increments an in-JVM counter. The plugin
 * also installs a small REST handler that exposes that counter so this test (running out-of-process) can observe rotation activity.
 */
public class EncryptedDataHandlerProviderSpiIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .name("test-encryption-spi-cluster")
        .plugin("test-encryption-spi-extension")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.encryption.key_rotation.interval", "1s")
        .setting("xpack.encryption.key_rotation.check_interval", "1s")
        .keystore("cluster.state.encryption.active_password_id", "v1")
        .keystore("cluster.state.encryption.password.v1", "encryption-test-password")
        .systemProperty("es.project_encryption_key_feature_flag_enabled", "true")
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

    public void testProviderIsDiscoveredAndHandlerIsInvoked() throws Exception {
        assertBusy(() -> {
            var response = client().performRequest(new Request("GET", "/_test/encryption_spi/invocations"));
            int count = assertOKAndCreateObjectPath(response).evaluate("invocations");
            assertThat(count, greaterThan(0));
        }, 30, TimeUnit.SECONDS);
    }
}
