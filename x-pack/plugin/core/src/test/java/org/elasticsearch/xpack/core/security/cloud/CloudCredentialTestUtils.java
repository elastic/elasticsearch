/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.cloud;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomByteArrayOfLength;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

/**
 * Shared test helpers for building random {@link PersistedCloudCredential} /
 * {@link CloudCredentialEncryptedData} instances, reused across the core, ML, and transform
 * test suites so the verbose envelope construction is not repeated.
 */
public final class CloudCredentialTestUtils {

    private CloudCredentialTestUtils() {}

    /**
     * A {@link PersistedCloudCredential} with a random id and a random encrypted payload.
     */
    public static PersistedCloudCredential randomPersistedCloudCredential() {
        return randomPersistedCloudCredential(randomAlphaOfLength(10));
    }

    /**
     * A {@link PersistedCloudCredential} with the given id and a random encrypted payload, for
     * tests that assert on (or key mocks by) a specific credential id.
     */
    public static PersistedCloudCredential randomPersistedCloudCredential(String id) {
        return new PersistedCloudCredential(id, randomCloudCredentialEncryptedData());
    }

    /**
     * {@link CloudCredentialEncryptedData} with a random key id and a random-length ciphertext.
     */
    public static CloudCredentialEncryptedData randomCloudCredentialEncryptedData() {
        return new CloudCredentialEncryptedData(randomAlphaOfLength(10), randomByteArrayOfLength(randomIntBetween(1, 32)));
    }
}
