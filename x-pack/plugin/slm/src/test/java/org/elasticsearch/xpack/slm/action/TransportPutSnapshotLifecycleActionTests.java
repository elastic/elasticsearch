/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceRegistry;
import org.elasticsearch.xpack.slm.IdentityEncryptionService;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class TransportPutSnapshotLifecycleActionTests extends ESTestCase {

    @Before
    public void registerEncryptionService() {
        EncryptionServiceRegistry.setEncryptionService(IdentityEncryptionService.INSTANCE);
    }

    @After
    public void resetEncryptionService() {
        EncryptionServiceRegistry.reset();
    }

    public void testPasswordIsEncryptedAndStrippedFromConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("indices", "data-*");
        config.put("encrypted_data", Map.of("type", "password", "password", "a-perfectly-valid-password", "password_id", "my-password-id"));
        SnapshotLifecyclePolicy policy = policyWithConfig(config);

        SnapshotLifecyclePolicy result = TransportPutSnapshotLifecycleAction.encryptPasswordIfPresent(policy);

        assertNotSame(policy, result);
        assertThat(result.getConfig().get("encrypted_data"), nullValue());
        assertThat(result.getConfig().get("indices"), equalTo("data-*"));
        assertThat(result.getEncryptedPassword(), notNullValue());
        assertEquals("a-perfectly-valid-password", new String(result.getEncryptedPassword().payload(), StandardCharsets.UTF_8));
        assertThat(result.getEncryptedPasswordId(), equalTo("my-password-id"));
    }

    public void testMissingPasswordIdIsRejected() {
        SnapshotLifecyclePolicy policy = policyWithConfig(
            Map.of("encrypted_data", Map.of("type", "password", "password", "a-perfectly-valid-password"))
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportPutSnapshotLifecycleAction.encryptPasswordIfPresent(policy)
        );
        assertThat(e.getMessage(), containsString("encrypted_data.password_id is required for SLM policies"));
    }

    public void testMalformedEncryptedDataIsRejected() {
        SnapshotLifecyclePolicy policy = policyWithConfig(Map.of("encrypted_data", "not-an-object"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportPutSnapshotLifecycleAction.encryptPasswordIfPresent(policy)
        );
        assertThat(e.getMessage(), containsString("malformed encrypted_data, should be an object"));
    }

    public void testPolicyWithoutPasswordIsReturnedByReference() {
        SnapshotLifecyclePolicy policy = policyWithConfig(Map.of("indices", "data-*"));
        assertThat(TransportPutSnapshotLifecycleAction.encryptPasswordIfPresent(policy), sameInstance(policy));

        SnapshotLifecyclePolicy noConfig = new SnapshotLifecyclePolicy("id", "snap", "0 30 1 * * ?", "repo", null, null);
        assertThat(TransportPutSnapshotLifecycleAction.encryptPasswordIfPresent(noConfig), sameInstance(noConfig));
    }

    private static SnapshotLifecyclePolicy policyWithConfig(Map<String, Object> config) {
        return new SnapshotLifecyclePolicy("id", "snap", "0 30 1 * * ?", "repo", config, null);
    }
}
