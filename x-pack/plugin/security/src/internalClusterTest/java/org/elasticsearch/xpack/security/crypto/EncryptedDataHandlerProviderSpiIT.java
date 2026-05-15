/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.crypto;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Verifies that {@link org.elasticsearch.encryption.EncryptedDataHandlerProvider} implementations are discovered via the
 * standard {@code META-INF/services} SPI mechanism and that their contributed handlers are invoked by the running
 * {@link KeyRotationCoordinator}.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, supportsDedicatedMasters = false)
public class EncryptedDataHandlerProviderSpiIT extends SecurityIntegTestCase {

    @Before
    public void checkFeatureFlag() {
        assumeTrue(
            "primary encryption key feature flag must be enabled",
            PrimaryEncryptionKeyService.PRIMARY_ENCRYPTION_KEY_FEATURE_FLAG.isEnabled()
        );
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(KeyRotationCoordinator.ROTATION_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .put(KeyRotationCoordinator.CHECK_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .build();
    }

    public void testProviderIsDiscoveredAndHandlerIsInvoked() throws Exception {
        int baseline = TestEncryptedDataHandlerProvider.INVOCATIONS.get();
        // If the SPI provider is discovered and its handler is registered with the coordinator, the next tick on the master will invoke
        // reEncrypt.
        assertBusy(() -> assertThat(TestEncryptedDataHandlerProvider.INVOCATIONS.get(), greaterThan(baseline)), 30, TimeUnit.SECONDS);
    }
}
