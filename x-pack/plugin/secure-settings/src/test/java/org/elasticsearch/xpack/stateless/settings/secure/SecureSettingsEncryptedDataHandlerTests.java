/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.stateless.settings.secure.SecureSettingsMetadataTests.randomEncryptedData;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SecureSettingsEncryptedDataHandlerTests extends ESTestCase {

    private final SecureSettingsEncryptedDataHandler handler = new SecureSettingsEncryptedDataHandler();

    public void testReEncryptRewrapsEveryValue() {
        EncryptedData a = randomEncryptedData();
        EncryptedData rewrapped = randomEncryptedData();
        Map<String, EncryptedData> settings = new HashMap<>();
        settings.put("a", a);
        settings.put("wiped", null);
        SecureSettingsMetadata current = new SecureSettingsMetadata(settings);

        SecureSettingsMetadata result = handler.reEncrypt(current, data -> data == a ? rewrapped : data);

        assertThat(result.settings().get("a"), sameInstance(rewrapped));
        assertTrue(result.settings().containsKey("wiped"));
        assertThat(result.settings().get("wiped"), nullValue());
    }

    public void testReEncryptReturnsSameInstanceWhenUnchanged() {
        SecureSettingsMetadata current = new SecureSettingsMetadata(Map.of("a", randomEncryptedData()));
        assertThat(handler.reEncrypt(current, data -> data), sameInstance(current));
    }

    public void testDestructiveResetKeepsNamesAndWipesValues() {
        SecureSettingsMetadata current = new SecureSettingsMetadata(Map.of("a", randomEncryptedData(), "b", randomEncryptedData()));

        SecureSettingsMetadata result = handler.onDestructiveReset(current);

        assertThat(result.settings().keySet(), equalTo(current.settings().keySet()));
        assertTrue(result.settings().values().stream().allMatch(v -> v == null));
        assertFalse(handler.hasData(result));
        assertTrue(handler.hasData(current));
    }

    public void testNotIncludedInSnapshots() {
        assertFalse(handler.includedInSnapshots());
    }
}
