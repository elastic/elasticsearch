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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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
     * Two stored secrets that differ only as ciphertext address one version, on the carrier production
     * actually stores. Encryption gives a fresh IV per write, so ciphertext is not a function of the secret:
     * re-encrypting an unchanged one changes it, and nothing here can tell that apart from a rotation. A
     * version that folded it in therefore versioned definitions that had not changed — two data sources
     * registered with the same settings shared no cache entry, and each probed every file for itself.
     * <p>
     * Credential isolation is not lost by this, it is placed where the plaintext is: {@code
     * Configured.secretIdentity}, which the provider digests after decryption and which is therefore equal
     * exactly when the secrets are, and the {@code StorageIdentity} stamped on the objects a provider reads.
     */
    public void testSecretsThatDifferOnlyAsCiphertextShareOneVersion() {
        String one = DefinitionVersion.of(
            dataset("s3://b/*.csv", Map.of("format", "csv")),
            encryptedSource("https://s3.example", "project-key-1", "ciphertext-of-AAA")
        );
        String other = DefinitionVersion.of(
            dataset("s3://b/*.csv", Map.of("format", "csv")),
            encryptedSource("https://s3.example", "project-key-1", "ciphertext-of-BBB")
        );
        assertEquals("a ciphertext must not version a definition that has not changed", one, other);
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

    /**
     * No credential value may appear in what the version is, since the version reaches a cache key.
     * <p>
     * Asserted structurally, because the digest is hexadecimal: searching its output for a secret would
     * be false for every implementation including one that folded the secret in verbatim, so it would
     * prove nothing. What is checked instead is that the whole value is hexadecimal — nothing that is
     * not a digest can survive into it.
     */
    public void testTheVersionCarriesNoCredentialValue() {
        String plaintext = DefinitionVersion.of(
            dataset("s3://b/*.csv", Map.of("format", "csv")),
            source(Map.of("endpoint", "https://s3.example", "access_key", "AKIAEXAMPLESECRET", "secret_key", "sh4redS3cret"))
        );
        assertTrue("the version must be nothing but a digest, got [" + plaintext + "]", plaintext.matches("[0-9a-f]{32}"));

        String encrypted = DefinitionVersion.of(
            dataset("s3://b/*.csv", Map.of("format", "csv")),
            encryptedSource("https://s3.example", "project-key-1", "ciphertext-material")
        );
        assertTrue("the version must be nothing but a digest, got [" + encrypted + "]", encrypted.matches("[0-9a-f]{32}"));
    }

    /**
     * A value can arrive as a {@code byte[]} — generic serialization round-trips one, which is why
     * {@code DataSourceSetting.equals} compares them by content. Rendered by identity instead, every
     * deserialization would mint a new version and the dataset would never warm.
     */
    public void testAByteArrayValueIsRenderedByContent() {
        Map<String, Object> settings = Map.of("format", "csv");
        DataSource first = new DataSource(
            "src",
            "s3",
            null,
            Map.of("endpoint", new DataSourceSetting("https://s3.example".getBytes(StandardCharsets.UTF_8), false))
        );
        DataSource second = new DataSource(
            "src",
            "s3",
            null,
            Map.of("endpoint", new DataSourceSetting("https://s3.example".getBytes(StandardCharsets.UTF_8), false))
        );
        assertEquals(
            "two equal byte[] values must produce one version, or nothing is ever warm",
            DefinitionVersion.of(dataset("s3://b/*.csv", settings), first),
            DefinitionVersion.of(dataset("s3://b/*.csv", settings), second)
        );
        DataSource different = new DataSource(
            "src",
            "s3",
            null,
            Map.of("endpoint", new DataSourceSetting("https://other.example".getBytes(StandardCharsets.UTF_8), false))
        );
        assertNotEquals(
            "a differing byte[] value must change the version",
            DefinitionVersion.of(dataset("s3://b/*.csv", settings), first),
            DefinitionVersion.of(dataset("s3://b/*.csv", settings), different)
        );
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

    // ---------------------------------------------------------------------------------------------------------
    // ofDataset: the DATASET-tier address. Everything above pins "which bytes, read how"; these pin "which
    // definition, in its entirety". The two differ deliberately on exactly two things - the names and the
    // declared mapping - so each of those is pinned here AND contrasted against #of, because a change that
    // collapsed the two values would pass either half alone.
    // ---------------------------------------------------------------------------------------------------------

    private static Dataset described(String description, Map<String, Object> settings) {
        return new Dataset("parts", new DataSourceReference("src"), "s3://b/*.csv", description, settings);
    }

    private static DatasetMapping declaring(DatasetMapping.Dynamic dynamic, Map<String, DatasetFieldMapping> properties) {
        return new DatasetMapping(new DatasetMapping.Mappings(dynamic, properties));
    }

    private static Dataset mapped(DatasetMapping mapping) {
        return new Dataset("parts", new DataSourceReference("src"), "s3://b/*.csv", null, Map.of("format", "csv"), mapping);
    }

    public void testTheDatasetVersionIsStableForTheSameDefinitions() {
        DataSource src = source(Map.of("endpoint", "https://s3.example"));
        assertEquals(
            DefinitionVersion.ofDataset(dataset("s3://b/*.csv", Map.of("format", "csv")), src),
            DefinitionVersion.ofDataset(dataset("s3://b/*.csv", Map.of("format", "csv")), src)
        );
    }

    /**
     * The first of the two divergences from {@link DefinitionVersion#of}. A fact about a FILE is reusable by any
     * dataset reading that file, so a rename must keep it warm; a fact about a DATASET is not, because two
     * datasets over identical bytes are still two datasets.
     */
    public void testRenamingEitherDefinitionMovesTheDatasetVersionButNotTheFileVersion() {
        DataSource src = source("src", Map.of("endpoint", "https://s3.example"));
        DataSource renamedSource = source("other", Map.of("endpoint", "https://s3.example"));
        Dataset parts = dataset("parts", "s3://b/*.csv", Map.of("format", "csv"));
        Dataset pieces = dataset("pieces", "s3://b/*.csv", Map.of("format", "csv"));

        assertNotEquals(
            "two datasets over identical bytes are still two datasets",
            DefinitionVersion.ofDataset(parts, src),
            DefinitionVersion.ofDataset(pieces, src)
        );
        assertNotEquals(
            "and so are two data sources",
            DefinitionVersion.ofDataset(parts, src),
            DefinitionVersion.ofDataset(parts, renamedSource)
        );
        assertEquals(
            "while the file-tier version still ignores both names, so a rename keeps per-file entries warm",
            DefinitionVersion.of(parts, src),
            DefinitionVersion.of(pieces, renamedSource)
        );
    }

    public void testEditingTheResourceMovesTheDatasetVersion() {
        DataSource src = source(Map.of("endpoint", "https://s3.example"));
        assertNotEquals(
            DefinitionVersion.ofDataset(dataset("s3://b/*.csv", Map.of("format", "csv")), src),
            DefinitionVersion.ofDataset(dataset("s3://b/other/*.csv", Map.of("format", "csv")), src)
        );
    }

    public void testEditingADatasetSettingMovesTheDatasetVersion() {
        DataSource src = source(Map.of("endpoint", "https://s3.example"));
        assertNotEquals(
            DefinitionVersion.ofDataset(dataset("s3://b/*.csv", Map.of("format", "csv")), src),
            DefinitionVersion.ofDataset(dataset("s3://b/*.csv", Map.of("format", "csv", "error_mode", "skip_row")), src)
        );
    }

    public void testEditingTheDataSourceMovesTheDatasetVersion() {
        Dataset parts = dataset("s3://b/*.csv", Map.of("format", "csv"));
        assertNotEquals(
            "an inherited setting decides what every dataset over the source reads",
            DefinitionVersion.ofDataset(parts, source(Map.of("endpoint", "https://s3.example"))),
            DefinitionVersion.ofDataset(parts, source(Map.of("endpoint", "https://s3.other")))
        );
        assertNotEquals(
            "and so does a rotated credential",
            DefinitionVersion.ofDataset(parts, encryptedSource("https://s3.example", "key-1", "cipher")),
            DefinitionVersion.ofDataset(parts, encryptedSource("https://s3.example", "key-2", "cipher"))
        );
    }

    /**
     * The second divergence from {@link DefinitionVersion#of}, and the one this change exists for. A declaration
     * decides which rows a read counts - one that drops rows under a lenient policy counts fewer of them - so a
     * count measured under one mapping is not another mapping's to serve. There is no version counter on either
     * metadata type to fold instead, so the mapping's content IS its version, and every component of it has to
     * move this value or an edit to that component silently reuses the previous definition's measurements.
     */
    public void testEveryPartOfADeclarationMovesTheDatasetVersion() {
        DataSource src = source(Map.of("endpoint", "https://s3.example"));
        DatasetMapping base = declaring(DatasetMapping.Dynamic.TRUE, Map.of("age", new DatasetFieldMapping("keyword", null)));
        String baseVersion = DefinitionVersion.ofDataset(mapped(base), src);

        assertNotEquals(
            "declaring anything at all",
            DefinitionVersion.ofDataset(dataset("s3://b/*.csv", Map.of("format", "csv")), src),
            baseVersion
        );
        assertNotEquals(
            "retyping a column",
            baseVersion,
            DefinitionVersion.ofDataset(
                mapped(declaring(DatasetMapping.Dynamic.TRUE, Map.of("age", new DatasetFieldMapping("long", null)))),
                src
            )
        );
        assertNotEquals(
            "renaming a column's source path",
            baseVersion,
            DefinitionVersion.ofDataset(
                mapped(declaring(DatasetMapping.Dynamic.TRUE, Map.of("age", new DatasetFieldMapping("keyword", "years")))),
                src
            )
        );
        assertNotEquals(
            "editing a column's date format",
            baseVersion,
            DefinitionVersion.ofDataset(
                mapped(
                    declaring(DatasetMapping.Dynamic.TRUE, Map.of("age", DatasetFieldMapping.withFormat("keyword", null, "yyyy-MM-dd")))
                ),
                src
            )
        );
        assertNotEquals(
            "declaring a second column",
            baseVersion,
            DefinitionVersion.ofDataset(
                mapped(
                    declaring(
                        DatasetMapping.Dynamic.TRUE,
                        Map.of("age", new DatasetFieldMapping("keyword", null), "name", new DatasetFieldMapping("keyword", null))
                    )
                ),
                src
            )
        );
        assertNotEquals(
            "changing the dynamic mode, which decides whether an undeclared column is read at all",
            baseVersion,
            DefinitionVersion.ofDataset(
                mapped(declaring(DatasetMapping.Dynamic.FALSE, Map.of("age", new DatasetFieldMapping("keyword", null)))),
                src
            )
        );
        assertEquals(
            "while the file-tier version still ignores the declaration entirely",
            DefinitionVersion.of(dataset("s3://b/*.csv", Map.of("format", "csv")), src),
            DefinitionVersion.of(mapped(base), src)
        );
    }

    /**
     * The one exclusion, and the reason the mechanism is not just "hash the whole object": a description changes
     * nothing a reader does, so editing one must not cost a cold scan of the dataset.
     */
    public void testEditingEitherDescriptionDoesNotMoveTheDatasetVersion() {
        Map<String, Object> settings = Map.of("format", "csv");
        DataSource sourceDescribed = new DataSource("src", "s3", "what this source is for", source(Map.of()).settings().asMap());
        DataSource sourceRedescribed = new DataSource("src", "s3", "something else entirely", source(Map.of()).settings().asMap());

        assertEquals(
            "a dataset's description decides nothing a reader does",
            DefinitionVersion.ofDataset(described("the parts corpus", settings), source(Map.of())),
            DefinitionVersion.ofDataset(described("the parts corpus, revised", settings), source(Map.of()))
        );
        assertEquals(
            "and neither does a data source's",
            DefinitionVersion.ofDataset(dataset("s3://b/*.csv", settings), sourceDescribed),
            DefinitionVersion.ofDataset(dataset("s3://b/*.csv", settings), sourceRedescribed)
        );
    }

    /**
     * A parsed mapping's iteration order is not part of the definition, for the same reason the settings are
     * sorted: the same declaration arriving in a different order must address the same entries.
     */
    public void testDeclaredColumnOrderDoesNotMoveTheDatasetVersion() {
        DataSource src = source(Map.of("endpoint", "https://s3.example"));
        Map<String, DatasetFieldMapping> ageFirst = new LinkedHashMap<>();
        ageFirst.put("age", new DatasetFieldMapping("keyword", null));
        ageFirst.put("name", new DatasetFieldMapping("long", null));
        Map<String, DatasetFieldMapping> nameFirst = new LinkedHashMap<>();
        nameFirst.put("name", new DatasetFieldMapping("long", null));
        nameFirst.put("age", new DatasetFieldMapping("keyword", null));

        assertEquals(
            DefinitionVersion.ofDataset(mapped(declaring(DatasetMapping.Dynamic.TRUE, ageFirst)), src),
            DefinitionVersion.ofDataset(mapped(declaring(DatasetMapping.Dynamic.TRUE, nameFirst)), src)
        );
    }

    /**
     * A declared column name is user-controlled text in the pre-image, so it gets the same length-prefix defence
     * the settings have. Without it, a name containing the encoding's own separators makes one declaration encode
     * identically to a different one, and two different datasets then share every address derived from it.
     * <p>
     * The forgery is constructed against the encoder rather than guessed, because a guessed one does not collide
     * and the case passes while proving nothing: a two-column declaration is impersonated by a ONE-column
     * declaration whose single name embeds the first column's remaining fields and the next column's {@code col}
     * marker. Length-prefixing the name is exactly what makes the two encodings differ, so dropping the prefix
     * makes this case fail - which is how it was checked.
     */
    public void testADeclaredColumnNameCannotForgeAFieldBoundary() {
        DataSource src = source(Map.of("endpoint", "https://s3.example"));
        // What `append` writes for the fields that follow the name of the first column, then the second's marker.
        String forged = "age" + "1:t7:keyword" + "1:p-1:" + "1:f-1:" + "col" + "zz";

        Map<String, DatasetFieldMapping> twoColumns = new LinkedHashMap<>();
        twoColumns.put("age", new DatasetFieldMapping("keyword", null));
        twoColumns.put("zz", new DatasetFieldMapping("long", null));

        assertNotEquals(
            "a declared column name must not be able to forge a field boundary",
            DefinitionVersion.ofDataset(mapped(declaring(DatasetMapping.Dynamic.TRUE, twoColumns)), src),
            DefinitionVersion.ofDataset(
                mapped(declaring(DatasetMapping.Dynamic.TRUE, Map.of(forged, new DatasetFieldMapping("long", null)))),
                src
            )
        );
    }

    public void testTheDatasetVersionIsFixedWidth() {
        DataSource src = source(Map.of("endpoint", "https://s3.example"));
        for (int i = 0; i < 64; i++) {
            String version = DefinitionVersion.ofDataset(dataset("s3://b/" + i + "/*.csv", Map.of("format", "csv")), src);
            assertEquals("a version that varied in width could be a prefix of another: " + version, 32, version.length());
        }
    }

    /**
     * The census, and the only case here that survives someone ADDING a field. Every other case pins a field
     * that exists today; this one fails the build when a new one appears, because the decision it then needs -
     * does this field change what a reader does? - cannot be made by a hash function and must not be made by
     * omission. A field left out of the fold is an edit that silently serves the previous definition's
     * measurements, which is the failure mode this tier has no other defence against: there is no version
     * counter to fold instead and no invalidation message, so the content is the whole of the protocol.
     */
    public void testTheFoldAccountsForEveryFieldOfBothDefinitions() {
        assertEquals(
            "a field was added to Dataset. Decide whether DefinitionVersion.ofDataset must fold it - anything that "
                + "changes what a reader does MUST - then list it here.",
            Set.of("name", "dataSource", "resource", "description", "settings", "mapping"),
            instanceFieldNames(Dataset.class)
        );
        assertEquals(
            "a field was added to DataSource. Same decision as above.",
            Set.of("name", "type", "description", "settings"),
            instanceFieldNames(DataSource.class)
        );
        assertEquals(
            "a field was added to a declared column. Same decision as above.",
            Set.of("type", "path", "format"),
            instanceFieldNames(DatasetFieldMapping.class)
        );
        assertEquals("a field was added to DatasetMapping.", Set.of("mappings"), instanceFieldNames(DatasetMapping.class));
        assertEquals(
            "a component was added to a mapping block.",
            Set.of("dynamic", "properties"),
            instanceFieldNames(DatasetMapping.Mappings.class)
        );
    }

    private static Set<String> instanceFieldNames(Class<?> type) {
        Set<String> names = new TreeSet<>();
        for (Field field : type.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) == false && field.isSynthetic() == false) {
                names.add(field.getName());
            }
        }
        return names;
    }
}
