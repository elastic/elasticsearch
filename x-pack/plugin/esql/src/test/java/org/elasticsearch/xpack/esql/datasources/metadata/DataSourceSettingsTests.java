/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.metadata;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class DataSourceSettingsTests extends ESTestCase {

    public void testHasSecretsEmpty() {
        assertFalse(DataSourceSettings.EMPTY.hasSecrets());
        assertFalse(new DataSourceSettings(Map.of()).hasSecrets());
    }

    public void testHasSecretsAllNonSecret() {
        Map<String, DataSourceSetting> in = new HashMap<>();
        in.put("region", new DataSourceSetting("us-east-1", false));
        in.put("max_retries", new DataSourceSetting(7, false));
        assertFalse(new DataSourceSettings(in).hasSecrets());
    }

    public void testHasSecretsNullValuedSecretDoesNotCount() {
        // A secret-classified setting with a null value carries nothing to protect, so hasSecrets()
        // stays false — the WARN/encrypt paths gate on this, so a null secret must not trip them.
        Map<String, DataSourceSetting> in = new HashMap<>();
        in.put("region", new DataSourceSetting("us-east-1", false));
        in.put("optional_secret", new DataSourceSetting(null, true));
        assertFalse(new DataSourceSettings(in).hasSecrets());
    }

    public void testHasSecretsRealSecret() {
        Map<String, DataSourceSetting> in = new HashMap<>();
        in.put("region", new DataSourceSetting("us-east-1", false));
        in.put("access_key", new DataSourceSetting("AKIA123", true));
        assertTrue(new DataSourceSettings(in).hasSecrets());
    }

    public void testToPresentationMapMasksSecrets() {
        Map<String, DataSourceSetting> in = new HashMap<>();
        in.put("region", new DataSourceSetting("us-east-1", false));
        in.put("access_key", new DataSourceSetting("AKIA123", true));
        in.put("optional_secret", new DataSourceSetting(null, true));

        Map<String, Object> masked = new DataSourceSettings(in).toPresentationMap();
        assertEquals("us-east-1", masked.get("region"));
        assertEquals(DataSourceSetting.MASK_SENTINEL, masked.get("access_key"));
        // A null-valued secret still presents as the sentinel (masking is by classification, not value).
        assertEquals(DataSourceSetting.MASK_SENTINEL, masked.get("optional_secret"));
        expectThrows(UnsupportedOperationException.class, () -> masked.put("x", "y"));
    }

    public void testConstructorDefensivelyCopies() {
        Map<String, DataSourceSetting> source = new HashMap<>();
        source.put("region", new DataSourceSetting("us-east-1", false));
        DataSourceSettings settings = new DataSourceSettings(source);

        // Mutating the source after construction must not leak into the "immutable" collection.
        source.put("injected", new DataSourceSetting("oops", false));
        assertEquals(1, settings.size());
        assertNull(settings.get("injected"));
        expectThrows(UnsupportedOperationException.class, () -> settings.asMap().put("x", new DataSourceSetting("v", false)));
    }

    public void testWriteableRoundTripWithMixedSettings() throws IOException {
        EncryptedData carrier = new EncryptedData("test-key", "AKIA123".getBytes(StandardCharsets.UTF_8));
        Map<String, DataSourceSetting> in = new HashMap<>();
        in.put("region", new DataSourceSetting("us-east-1", false));
        in.put("access_key", new DataSourceSetting(carrier, true));
        in.put("plaintext_secret", new DataSourceSetting("AKIA", true));
        DataSourceSettings settings = new DataSourceSettings(in);

        BytesStreamOutput out = new BytesStreamOutput();
        settings.writeTo(out);
        StreamInput sin = out.bytes().streamInput();
        DataSourceSettings deserialized = new DataSourceSettings(sin);

        assertEquals(settings, deserialized);
        assertTrue(deserialized.get("access_key").isEncrypted());
        assertEquals(carrier, deserialized.get("access_key").rawValue());
        assertTrue(deserialized.hasSecrets());
    }
}
