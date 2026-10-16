/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;

import java.util.Map;

public class FileDataSourceConfigurationTests extends ESTestCase {

    /**
     * The inline-WITH gate in {@code StorageProviderRegistry} calls {@link FileDataSourceConfiguration#isManagedIdentityAuth}
     * on the raw, pre-canonicalization {@code auth} value, so it must accept both the canonical {@code managed_identity}
     * and the deprecated {@code workload_identity} alias, case-insensitively, and nothing else.
     */
    public void testIsManagedIdentityAuthAcceptsCanonicalAndDeprecatedAlias() {
        assertTrue(FileDataSourceConfiguration.isManagedIdentityAuth("managed_identity"));
        assertTrue(FileDataSourceConfiguration.isManagedIdentityAuth("MANAGED_IDENTITY"));
        // Deprecated alias, matched on the raw value before canonicalization.
        assertTrue(FileDataSourceConfiguration.isManagedIdentityAuth("workload_identity"));
        assertTrue(FileDataSourceConfiguration.isManagedIdentityAuth("Workload_Identity"));
    }

    public void testIsManagedIdentityAuthRejectsOtherValues() {
        assertFalse(FileDataSourceConfiguration.isManagedIdentityAuth("anonymous"));
        assertFalse(FileDataSourceConfiguration.isManagedIdentityAuth("auto"));
        assertFalse(FileDataSourceConfiguration.isManagedIdentityAuth("federated_identity"));
        assertFalse(FileDataSourceConfiguration.isManagedIdentityAuth("static_credentials"));
        assertFalse(FileDataSourceConfiguration.isManagedIdentityAuth(null));
        assertFalse(FileDataSourceConfiguration.isManagedIdentityAuth(42));
    }

    public void testDatasetAuditSettings() {
        for (var field : FileDataSourceValidator.DATASET_FIELDS) {
            var request = new PutDatasetAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                "dataset",
                "datasource",
                "resource",
                null,
                Map.of(field, "test-value")
            );

            assertEquals(
                "Unexpected audit value for setting [" + field + "]",
                "test-value",
                request.auditFields().get("settings." + field)
            );
        }
    }
}
