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
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
     * A declared column name is user-controlled text in a VALUE slot, so what defends it is the value length
     * prefix. The forgery is constructed against the encoder rather than guessed, because a guessed one does not
     * collide and the case then proves nothing: a two-column declaration is impersonated by a one-column one
     * whose single name embeds the first column's remaining fields and the next column's {@code col} marker.
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

    /**
     * The two settings blocks are separated only by the fixed {@code ("type", …)} pair, so a key named
     * {@code type} could otherwise bridge them: a dataset with no settings over an {@code s3} source encodes
     * like a dataset whose settings are {@code {"type":"s3"}} over a source of another type. Count-prefixing each
     * block is what makes it self-delimiting. No registered key is named {@code type} today, which made this
     * unreachable rather than safe - and nothing enforces that it stays unreachable.
     */
    public void testASettingKeyCannotBridgeTheTwoSettingsBlocks() {
        // No dataset settings, source type s3, and the source carrying type=gcs ...
        Dataset bare = dataset("s3://b/*.csv", Map.of());
        DataSource s3WithGcsSetting = new DataSource("src", "s3", null, Map.of("type", new DataSourceSetting("gcs", false)));
        // ... encodes, unseparated, exactly like type=s3 in the DATASET's settings over a gcs source.
        Dataset carriesType = dataset("s3://b/*.csv", Map.of("type", "s3"));
        DataSource gcsBare = new DataSource("src", "gcs", null, Map.<String, DataSourceSetting>of());

        assertNotEquals(
            "a setting named like the fixed field between the two blocks must not merge them",
            DefinitionVersion.ofDataset(bare, s3WithGcsSetting),
            DefinitionVersion.ofDataset(carriesType, gcsBare)
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
     * The census, and the only case here that survives someone ADDING a field. The decision a new field needs -
     * does it change what a reader does? - cannot be made by a hash function and must not be made by omission.
     * <p>
     * Read from the SOURCE, because {@code getDeclaredFields} is a forbidden API here and {@code getFields} cannot
     * see a private field. It is also the better instrument: a declaration is what a person adds.
     */
    public void testTheFoldAccountsForEveryFieldOfBothDefinitions() throws Exception {
        assertEquals(
            "a field was added to Dataset. Decide whether DefinitionVersion.ofDataset must fold it - anything that "
                + "changes what a reader does MUST - then list it here. This asserts the DECLARED set, not that "
                + "each one is folded: dataSource is folded as the resolved parent's name, and description is "
                + "folded by nothing on purpose.",
            Set.of("name", "dataSource", "resource", "description", "settings", "mapping"),
            declaredFieldsOf("server/src/main/java/org/elasticsearch/cluster/metadata/Dataset.java", "Dataset")
        );
        assertEquals(
            "a field was added to DataSource. Same decision as above.",
            Set.of("name", "type", "description", "settings"),
            declaredFieldsOf(
                "x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/metadata/DataSource.java",
                "DataSource"
            )
        );
        assertEquals(
            "a field was added to a declared column. Same decision as above.",
            Set.of("type", "path", "format"),
            declaredFieldsOf("server/src/main/java/org/elasticsearch/cluster/metadata/DatasetFieldMapping.java", "DatasetFieldMapping")
        );
        assertEquals(
            "a field or mapping-block component was added to DatasetMapping.",
            Set.of("mappings", "dynamic", "properties"),
            declaredFieldsOf("server/src/main/java/org/elasticsearch/cluster/metadata/DatasetMapping.java", "DatasetMapping")
        );
        // One level below DataSource.settings, which is where the fold actually reaches. `secret` is deliberately
        // NOT folded: it decides whether a value is masked on read-back, and mergeSettings yields the same merged
        // config for a given rawValue either way, so it changes nothing a reader does.
        assertEquals(
            "a field was added to a data source setting. Same decision as above.",
            Set.of("value", "secret"),
            declaredFieldsOf(
                "x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/metadata/DataSourceSetting.java",
                "DataSourceSetting"
            )
        );
    }

    /** Instance fields and record components declared in one source file. */
    private static Set<String> declaredFieldsOf(String relativePath, String simpleName) throws IOException {
        Path root = PathUtils.get("").toAbsolutePath();
        for (int i = 0; i < 12 && root != null && Files.exists(root.resolve(relativePath)) == false; i++) {
            root = root.getParent();
        }
        assertNotNull("cannot locate " + relativePath + " from " + PathUtils.get("").toAbsolutePath(), root);
        Set<String> names = declaredFieldsIn(Files.readString(root.resolve(relativePath), StandardCharsets.UTF_8));
        assertFalse(simpleName + " declares no fields - the census would pass vacuously", names.isEmpty());
        return names;
    }

    /**
     * The parser, separated from the file it reads so {@link #testTheFieldScannerSeesBothShapes} can hold it to a
     * fixture. It was wrong once - splitting a record header on every comma tore {@code Map<String, V>} in half -
     * and a scanner that silently finds the wrong set makes the census above pass while checking nothing.
     */
    static Set<String> declaredFieldsIn(String source) {
        Set<String> names = new TreeSet<>();
        Matcher field = Pattern.compile("^\\s{4}private final [\\w<>,\\[\\] ?.]+ (\\w+);", Pattern.MULTILINE).matcher(source);
        while (field.find()) {
            names.add(field.group(1));
        }
        Matcher rec = Pattern.compile("record \\w+\\(([^)]*)\\)", Pattern.DOTALL).matcher(source);
        while (rec.find()) {
            // Split on commas at angle-bracket depth 0: a component's own type may carry one, as
            // Map<String, DatasetFieldMapping> does.
            String header = rec.group(1);
            int depth = 0;
            int start = 0;
            for (int i = 0; i <= header.length(); i++) {
                char ch = i < header.length() ? header.charAt(i) : ',';
                if (ch == '<') {
                    depth++;
                } else if (ch == '>') {
                    depth--;
                } else if (ch == ',' && depth == 0) {
                    String trimmed = header.substring(start, i).trim().replaceAll("@\\w+\\s+", "");
                    if (trimmed.isEmpty() == false) {
                        names.add(trimmed.substring(trimmed.lastIndexOf(' ') + 1));
                    }
                    start = i + 1;
                }
            }
        }
        return names;
    }

    public void testTheFieldScannerSeesBothShapes() {
        String source = """
            public final class Thing {
                private static final String IGNORED = "not an instance field";
                private final String name;
                @Nullable
                private final Map<String, List<Integer>> settings;
                private final String withInitializer = "x";

                public record Inner(Dynamic dynamic, Map<String, Field> properties) {}
            }
            """;
        assertEquals(
            "a plain field, a generic field, and both components of a record whose type carries a comma",
            Set.of("name", "settings", "dynamic", "properties"),
            declaredFieldsIn(source)
        );
    }
}
