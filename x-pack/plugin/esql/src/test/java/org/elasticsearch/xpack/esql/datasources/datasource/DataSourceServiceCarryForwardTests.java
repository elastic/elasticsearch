/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link DataSourceService#isUntouchedSecret}, the predicate deciding whether a stored secret
 * should carry forward into a PUT-as-update. Two regressions found in review: a request that explicitly
 * supplies a value (even an empty one) for a secret must stand on its own rather than inheriting "still
 * there" credit from the value it's replacing; and a secret already wiped (e.g. by a destructive encryption
 * reset, stored with a {@code null} value) must never count as carried forward.
 */
public class DataSourceServiceCarryForwardTests extends ESTestCase {

    public void testAbsentSecretIsUntouched() {
        DataSourceSetting stored = new DataSourceSetting("s3cr3t", true);
        assertTrue(DataSourceService.isUntouchedSecret(stored, "secret_key", Map.of()));
    }

    public void testExplicitNonNullValueIsNotUntouched() {
        // A supplied value (even an empty string) must stand on its own, not inherit carry-forward credit
        // from the stored value it's replacing.
        DataSourceSetting stored = new DataSourceSetting("s3cr3t", true);
        Map<String, Object> raw = new HashMap<>();
        raw.put("secret_key", "");
        assertFalse(DataSourceService.isUntouchedSecret(stored, "secret_key", raw));
    }

    public void testExplicitNullValueIsNotUntouched() {
        DataSourceSetting stored = new DataSourceSetting("s3cr3t", true);
        Map<String, Object> raw = new HashMap<>();
        raw.put("secret_key", null);
        assertFalse(DataSourceService.isUntouchedSecret(stored, "secret_key", raw));
    }

    public void testWipedSecretIsNeverUntouched() {
        // A secret already wiped (present but null-valued, e.g. after a destructive encryption reset meant
        // to force re-entry of credentials) has nothing to carry forward, regardless of the request.
        DataSourceSetting wiped = new DataSourceSetting(null, true);
        assertFalse(DataSourceService.isUntouchedSecret(wiped, "secret_key", Map.of()));
    }

    public void testNonSecretFieldIsNeverUntouched() {
        DataSourceSetting nonSecret = new DataSourceSetting("us-east-1", false);
        assertFalse(DataSourceService.isUntouchedSecret(nonSecret, "region", Map.of()));
    }
}
