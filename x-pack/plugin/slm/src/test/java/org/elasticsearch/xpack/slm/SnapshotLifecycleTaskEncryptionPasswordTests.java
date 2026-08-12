/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.encryption.EncryptedData;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceRegistry;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.hamcrest.Matchers.nullValue;

public class SnapshotLifecycleTaskEncryptionPasswordTests extends ESTestCase {

    @Before
    public void registerEncryptionService() {
        EncryptionServiceRegistry.setEncryptionService(IdentityEncryptionService.INSTANCE);
    }

    @After
    public void resetEncryptionService() {
        EncryptionServiceRegistry.reset();
    }

    public void testPolicyPasswordIsDecryptedOntoTheRequest() {
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
            "id",
            "snap",
            "0 30 1 * * ?",
            "repo",
            Map.of("indices", "data-*"),
            null,
            null,
            new EncryptedData("test-key", "a-perfectly-valid-password".getBytes(StandardCharsets.UTF_8)),
            "my-password-id"
        );
        CreateSnapshotRequest request = new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, "repo", "snap");

        SnapshotLifecycleTask.setEncryptedData(policy, request);

        assertEquals("a-perfectly-valid-password", request.encryptedData().password().toString());
        assertEquals("my-password-id", request.encryptedData().passwordId());
    }

    public void testPolicyWithoutPasswordLeavesRequestUntouched() {
        CreateSnapshotRequest request = new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, "repo", "snap");

        SnapshotLifecycleTask.setEncryptedData(policy(), request);

        assertThat(request.encryptedData(), nullValue());
    }

    private static SnapshotLifecyclePolicy policy() {
        return new SnapshotLifecyclePolicy("id", "snap", "0 30 1 * * ?", "repo", Map.of("indices", "data-*"), null);
    }
}
