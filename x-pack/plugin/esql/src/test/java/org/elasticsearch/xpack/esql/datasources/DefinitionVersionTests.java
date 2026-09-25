/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The version exists so that everything derived from a dataset's definitions is addressed by those
 * definitions. These cases pin both directions: an edit must change it, and an unchanged definition
 * must not. Only the second keeps the mechanism from being equivalent to disabling the cache.
 */
public class DefinitionVersionTests extends ESTestCase {

    /** Plaintext settings, as a data source registered while encryption is unavailable stores them. */
    private static DataSource source(Map<String, Object> settings) {
        return source("src", settings);
    }

    private static DataSource source(String name, Map<String, Object> settings) {
        Map<String, DataSourceSetting> wrapped = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : settings.entrySet()) {
            boolean secret = e.getKey().contains("key") || e.getKey().contains("token");
            wrapped.put(e.getKey(), new DataSourceSetting(e.getValue(), secret));
        }
        return new DataSource(name, "s3", null, wrapped);
    }

    /**
     * A data source as it is actually stored: a secret is an {@link EncryptedData} carrier, not a
     * {@link String}. This is the shape the production path produces, so it is the shape the rotation
     * case below has to be built on — over a plaintext carrier that case passes without proving
     * anything about a real rotation.
     */
    private static DataSource encryptedSource(String endpoint, String keyId, String secretCiphertext) {
        Map<String, DataSourceSetting> wrapped = new LinkedHashMap<>();
        wrapped.put("endpoint", new DataSourceSetting(endpoint, false));
        wrapped.put("secret_key", new DataSourceSetting(new EncryptedData(keyId, secretCiphertext.getBytes(StandardCharsets.UTF_8)), true));
        return new DataSource("src", "s3", null, wrapped);
    }

    private static Dataset dataset(String resource, Map<String, Object> settings) {
        return dataset("parts", resource, settings);
    }

    private static Dataset dataset(String name, String resource, Map<String, Object> settings) {
        return new Dataset(name, new DataSourceReference("src"), resource, null, settings);
    }

    public void testSameDefinitionsProduceTheSameVersion() {
        Map<String, Object> dsSettings = Map.of("format", "csv", "error_mode", "null_field");
        Map<String, Object> srcSettings = Map.of("endpoint", "https://s3.example", "access_key", "AAA");

        String a = DefinitionVersion.of(dataset("s3://b/*.csv", dsSettings), source(srcSettings));
        String b = DefinitionVersion.of(dataset("s3://b/*.csv", dsSettings), source(srcSettings));
        assertEquals("an unchanged definition keeps its version, or nothing is ever warm", a, b);
    }

    public void testSettingOrderDoesNotChangeTheVersion() {
        Map<String, Object> ordered = new LinkedHashMap<>();
        ordered.put("format", "csv");
        ordered.put("error_mode", "null_field");
        Map<String, Object> reversed = new LinkedHashMap<>();
        reversed.put("error_mode", "null_field");
        reversed.put("format", "csv");

        assertEquals(
            "the version is of the definition, not of the order it happened to be stored in",
            DefinitionVersion.of(dataset("s3://b/*.csv", ordered), source(Map.of("endpoint", "e"))),
            DefinitionVersion.of(dataset("s3://b/*.csv", reversed), source(Map.of("endpoint", "e")))
        );
    }

    /**
     * A name decides nothing about what is read, so two definitions equal in content address one set of
     * entries. Without this, N datasets over one prefix each hold their own copy of the listing and each
     * issue their own LIST, and a rename throws away a warm cache.
     */
    public void testNamesDoNotChangeTheVersion() {
        Map<String, Object> dsSettings = Map.of("format", "csv");
        Map<String, Object> srcSettings = Map.of("endpoint", "https://s3.example");
        assertEquals(
            "two datasets equal in content must share their derived entries",
            DefinitionVersion.of(dataset("parts_a", "s3://b/*.csv", dsSettings), source("src_one", srcSettings)),
            DefinitionVersion.of(dataset("parts_b", "s3://b/*.csv", dsSettings), source("src_two", srcSettings))
        );
    }

    public void testEditingADatasetSettingChangesTheVersion() {
        DataSource src = source(Map.of("endpoint", "https://s3.example"));
        String before = DefinitionVersion.of(dataset("s3://b/*.csv", Map.of("format", "csv", "error_mode", "fail_fast")), src);
        String after = DefinitionVersion.of(dataset("s3://b/*.csv", Map.of("format", "csv", "error_mode", "null_field")), src);
        assertNotEquals("an edited setting must take what was derived under the old one out of reach", before, after);
    }

    public void testEditingTheResourceChangesTheVersion() {
        DataSource src = source(Map.of("endpoint", "https://s3.example"));
        Map<String, Object> settings = Map.of("format", "csv");
        assertNotEquals(
            DefinitionVersion.of(dataset("s3://b/*.csv", settings), src),
            DefinitionVersion.of(dataset("s3://b/other/*.csv", settings), src)
        );
    }

    /**
     * The case this mechanism exists for, on the carrier production actually stores. An
     * {@link EncryptedData}'s {@code toString} redacts its ciphertext, so a version that let the carrier
     * render itself would be identical across a rotation and every entry harvested under the old
     * credential would stay addressable.
     */
    public void testRotatingAnEncryptedCredentialChangesTheVersion() {
        String before = DefinitionVersion.of(
            dataset("s3://b/*.csv", Map.of("format", "csv")),
            encryptedSource("https://s3.example", "project-key-1", "ciphertext-of-AAA")
        );
        String after = DefinitionVersion.of(
            dataset("s3://b/*.csv", Map.of("format", "csv")),
            encryptedSource("https://s3.example", "project-key-1", "ciphertext-of-BBB")
        );
        assertNotEquals("a rotation under one encryption key must still change the version", before, after);
    }

    /** Re-keying without changing the secret is also a change to what is stored, and also invalidates. */
    public void testChangingTheEncryptionKeyChangesTheVersion() {
        assertNotEquals(
            DefinitionVersion.of(
                dataset("s3://b/*.csv", Map.of("format", "csv")),
                encryptedSource("https://s3.example", "project-key-1", "same-ciphertext")
            ),
            DefinitionVersion.of(
                dataset("s3://b/*.csv", Map.of("format", "csv")),
                encryptedSource("https://s3.example", "project-key-2", "same-ciphertext")
            )
        );
    }

    /** The same case on the plaintext carrier, which is what a cluster without encryption stores. */
    public void testRotatingAPlaintextCredentialChangesTheVersion() {
        Map<String, Object> settings = Map.of("format", "csv");
        String before = DefinitionVersion.of(
            dataset("s3://b/*.csv", settings),
            source(Map.of("endpoint", "https://s3.example", "access_key", "AAA", "secret_key", "BBB"))
        );
        String after = DefinitionVersion.of(
            dataset("s3://b/*.csv", settings),
            source(Map.of("endpoint", "https://s3.example", "access_key", "CCC", "secret_key", "DDD"))
        );
        assertNotEquals("a credential rotation must take what was derived under the old one out of reach", before, after);
    }

    /** A dataset inherits its data source's settings, so an edit to the source reaches the dataset's version. */
    public void testEditingTheDataSourceEndpointChangesTheVersion() {
        Map<String, Object> settings = Map.of("format", "csv");
        assertNotEquals(
            DefinitionVersion.of(dataset("s3://b/*.csv", settings), source(Map.of("endpoint", "https://s3.example"))),
            DefinitionVersion.of(dataset("s3://b/*.csv", settings), source(Map.of("endpoint", "https://other.example")))
        );
    }

    /** No credential value may appear in what the version is, since the version reaches a cache key. */
    public void testTheVersionCarriesNoCredentialValue() {
        String plaintext = DefinitionVersion.of(
            dataset("s3://b/*.csv", Map.of("format", "csv")),
            source(Map.of("endpoint", "https://s3.example", "access_key", "AKIAEXAMPLESECRET", "secret_key", "sh4redS3cret"))
        );
        assertFalse("a credential must not survive into the version", plaintext.contains("AKIAEXAMPLESECRET"));
        assertFalse("a credential must not survive into the version", plaintext.contains("sh4redS3cret"));

        String encrypted = DefinitionVersion.of(
            dataset("s3://b/*.csv", Map.of("format", "csv")),
            encryptedSource("https://s3.example", "project-key-1", "ciphertext-material")
        );
        assertFalse("not even the ciphertext survives into the version", encrypted.contains("ciphertext-material"));
        assertFalse("nor the encryption key id", encrypted.contains("project-key-1"));
    }

    /**
     * Fixed width, because the halves of the digest are concatenated: unpadded, {@code (0x1, 0x23)} and
     * {@code (0x12, 0x3)} both render {@code "123"}, and two definitions rendering to one version share
     * every cache address.
     */
    public void testTheVersionIsFixedWidth() {
        for (int i = 0; i < 200; i++) {
            String version = DefinitionVersion.of(
                dataset("s3://b/" + randomAlphaOfLength(8) + "/*.csv", Map.of("format", "csv")),
                source(Map.of("endpoint", "https://" + randomAlphaOfLength(6) + ".example"))
            );
            assertEquals("the two 64-bit halves are each zero-padded to 16 hex digits: " + version, 32, version.length());
        }
    }

    /**
     * A setting value cannot forge a field boundary. The pair below collides under an unprefixed
     * {@code name=value\0} encoding: one setting whose value embeds a separator and a name is
     * indistinguishable from two settings.
     */
    public void testASettingValueCannotForgeAFieldBoundary() {
        DataSource src = source(Map.of("endpoint", "https://s3.example"));
        Map<String, Object> twoSettings = new LinkedHashMap<>();
        twoSettings.put("a", "1");
        twoSettings.put("b", "2");
        Map<String, Object> oneForgedSetting = Map.of("a", "1\u0000b=2");

        assertNotEquals(
            "a value that embeds a separator must not encode as two settings",
            DefinitionVersion.of(dataset("s3://b/*.csv", twoSettings), src),
            DefinitionVersion.of(dataset("s3://b/*.csv", oneForgedSetting), src)
        );
    }

    /**
     * A mapping decides how bytes become rows, not which bytes a query can reach, so it is not part of
     * this identity. A dataset declaring exactly what inference already produces describes the same read
     * and must keep sharing the entries of its undeclared twin — otherwise every mapped dataset pays a
     * cold scan for declaring nothing. The isolation that two genuinely different reads need is the read
     * configuration's job, not this value's.
     */
    public void testADeclaredMappingDoesNotChangeTheVersion() {
        DataSource src = source(Map.of("endpoint", "https://s3.example"));
        Map<String, Object> settings = Map.of("format", "csv");
        DatasetMapping declared = new DatasetMapping(
            new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, Map.of("age", new DatasetFieldMapping("keyword", null)))
        );

        Dataset undeclared = new Dataset("parts", new DataSourceReference("src"), "s3://b/*.csv", null, settings);
        Dataset redeclared = new Dataset("parts", new DataSourceReference("src"), "s3://b/*.csv", null, settings, declared);

        assertEquals(
            "a declaration describes a read, not a different set of bytes",
            DefinitionVersion.of(undeclared, src),
            DefinitionVersion.of(redeclared, src)
        );
    }
}
