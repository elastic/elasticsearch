/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class TransportRepositoryVerifyIntegrityActionTests extends ESTestCase {
    public void testEnsureValidGenId() {
        TransportRepositoryVerifyIntegrityAction.ensureValidGenId(0);
        TransportRepositoryVerifyIntegrityAction.ensureValidGenId(randomNonNegativeLong());
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> TransportRepositoryVerifyIntegrityAction.ensureValidGenId(RepositoryData.EMPTY_REPO_GEN)
            ).getMessage(),
            equalTo("repository is empty, cannot verify its integrity")
        );
        assertThat(expectThrows(IllegalStateException.class, () -> {
            try {
                TransportRepositoryVerifyIntegrityAction.ensureValidGenId(RepositoryData.CORRUPTED_REPO_GEN);
            } catch (AssertionError e) {
                // if assertions disabled, we throw the cause directly
                throw e.getCause();
            }
        }).getMessage(), equalTo("repository is in an unexpected state [-3], cannot verify its integrity"));
    }
}
