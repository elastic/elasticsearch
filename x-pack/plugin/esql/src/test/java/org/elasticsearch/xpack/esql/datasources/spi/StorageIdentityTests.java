/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class StorageIdentityTests extends ESTestCase {

    public void testUniqueIdentitiesNeverShare() {
        StorageIdentity identity = StorageIdentity.unique();
        assertThat(identity, equalTo(identity));
        assertThat(identity, not(equalTo(StorageIdentity.unique())));
    }

    public void testDigestSecretIsStableAndDistinguishesSecrets() {
        String secret = randomAlphaOfLength(20);
        assertThat(StorageIdentity.digestSecret(secret), equalTo(StorageIdentity.digestSecret(secret)));
        assertThat(StorageIdentity.digestSecret(secret), not(equalTo(StorageIdentity.digestSecret(secret + "x"))));
        assertFalse(StorageIdentity.digestSecret(secret).contains(secret));
    }

    public void testDigestSecretKeepsNull() {
        assertThat(StorageIdentity.digestSecret(null), nullValue());
    }
}
