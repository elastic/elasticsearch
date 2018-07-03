/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class HashProcessorFactoryTests extends ESTestCase {

    public void testProcessor() {
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("xpack.security.ingest.hash.processor.key", "my_key");
        Settings settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
        HashProcessor.Factory factory = new HashProcessor.Factory(settings);
        Map<String, Object> config = new HashMap<>();
        config.put("fields", Collections.singletonList("_field"));
        config.put("target_field", "_target");
        config.put("salt", "_salt");
        config.put("key_setting", "xpack.security.ingest.hash.processor.key");
        for (HashProcessor.Method method : HashProcessor.Method.values()) {
            config.put("method", method.toString());
            HashProcessor processor = factory.create(null, "_tag", new HashMap<>(config));
            assertThat(processor.getFields(), equalTo(Collections.singletonList("_field")));
            assertThat(processor.getTargetField(), equalTo("_target"));
            assertArrayEquals(processor.getSalt(), "_salt".getBytes(StandardCharsets.UTF_8));
        }
    }

    public void testProcessorNoFields() {
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("xpack.security.ingest.hash.processor.key", "my_key");
        Settings settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
        HashProcessor.Factory factory = new HashProcessor.Factory(settings);
        Map<String, Object> config = new HashMap<>();
        config.put("target_field", "_target");
        config.put("salt", "_salt");
        config.put("key_setting", "xpack.security.ingest.hash.processor.key");
        config.put("method", HashProcessor.Method.SHA1.toString());
        ElasticsearchException e = expectThrows(ElasticsearchException.class,
            () -> factory.create(null, "_tag", config));
        assertThat(e.getMessage(), equalTo("[fields] required property is missing"));
    }

    public void testProcessorNoTargetField() {
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("xpack.security.ingest.hash.processor.key", "my_key");
        Settings settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
        HashProcessor.Factory factory = new HashProcessor.Factory(settings);
        Map<String, Object> config = new HashMap<>();
        config.put("fields", Collections.singletonList("_field"));
        config.put("salt", "_salt");
        config.put("key_setting", "xpack.security.ingest.hash.processor.key");
        config.put("method", HashProcessor.Method.SHA1.toString());
        ElasticsearchException e = expectThrows(ElasticsearchException.class,
            () -> factory.create(null, "_tag", config));
        assertThat(e.getMessage(), equalTo("[target_field] required property is missing"));
    }

    public void testProcessorFieldsIsEmpty() {
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("xpack.security.ingest.hash.processor.key", "my_key");
        Settings settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
        HashProcessor.Factory factory = new HashProcessor.Factory(settings);
        Map<String, Object> config = new HashMap<>();
        config.put("fields", Collections.singletonList(randomBoolean() ? "" : null));
        config.put("salt", "_salt");
        config.put("target_field", "_target");
        config.put("key_setting", "xpack.security.ingest.hash.processor.key");
        config.put("method", HashProcessor.Method.SHA1.toString());
        ElasticsearchException e = expectThrows(ElasticsearchException.class,
            () -> factory.create(null, "_tag", config));
        assertThat(e.getMessage(), equalTo("[fields] a field-name entry is either empty or null"));
    }

    public void testProcessorMissingSalt() {
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("xpack.security.ingest.hash.processor.key", "my_key");
        Settings settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
        HashProcessor.Factory factory = new HashProcessor.Factory(settings);
        Map<String, Object> config = new HashMap<>();
        config.put("fields", Collections.singletonList("_field"));
        config.put("target_field", "_target");
        config.put("key_setting", "xpack.security.ingest.hash.processor.key");
        ElasticsearchException e = expectThrows(ElasticsearchException.class,
            () -> factory.create(null, "_tag", config));
        assertThat(e.getMessage(), equalTo("[salt] required property is missing"));
    }

    public void testProcessorInvalidMethod() {
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("xpack.security.ingest.hash.processor.key", "my_key");
        Settings settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
        HashProcessor.Factory factory = new HashProcessor.Factory(settings);
        Map<String, Object> config = new HashMap<>();
        config.put("fields", Collections.singletonList("_field"));
        config.put("salt", "_salt");
        config.put("target_field", "_target");
        config.put("key_setting", "xpack.security.ingest.hash.processor.key");
        config.put("method", "invalid");
        ElasticsearchException e = expectThrows(ElasticsearchException.class,
            () -> factory.create(null, "_tag", config));
        assertThat(e.getMessage(), equalTo("[method] type [invalid] not supported, cannot convert field. " +
            "Valid hash methods: [sha1, sha256, sha384, sha512]"));
    }

    public void testProcessorInvalidOrMissingKeySetting() {
        Settings settings = Settings.builder().setSecureSettings(new MockSecureSettings()).build();
        HashProcessor.Factory factory = new HashProcessor.Factory(settings);
        Map<String, Object> config = new HashMap<>();
        config.put("fields", Collections.singletonList("_field"));
        config.put("salt", "_salt");
        config.put("target_field", "_target");
        config.put("key_setting", "invalid");
        config.put("method", HashProcessor.Method.SHA1.toString());
        ElasticsearchException e = expectThrows(ElasticsearchException.class,
            () -> factory.create(null, "_tag", new HashMap<>(config)));
        assertThat(e.getMessage(),
            equalTo("[key_setting] key [invalid] must match [xpack.security.ingest.hash.*.key]. It is not set"));
        config.remove("key_setting");
        ElasticsearchException ex = expectThrows(ElasticsearchException.class,
            () -> factory.create(null, "_tag", config));
        assertThat(ex.getMessage(), equalTo("[key_setting] required property is missing"));
    }
}
