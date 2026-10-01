/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.esql.datasources.DefinitionVersion;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Every cache addressing something derived from a dataset's definitions carries the version of those
 * definitions, so two datasets that read under different identities cannot reach each other's entries.
 * <p>
 * Before this, each key hashed credentials from its own list of seven setting names — {@code access_key},
 * {@code secret_key}, {@code connection_string}, {@code key}, {@code sas_token}, {@code credentials},
 * {@code token} — duplicated between two of them and omitting {@code session_token}, {@code role_arn} and
 * {@code auth}. Two data sources assuming different roles over one bucket therefore produced identical
 * keys, and whichever queried second was served the file set the first identity could see. Adding the
 * missing names would have fixed the reported pair and left the mechanism: the next authentication
 * setting has to be remembered too, and forgetting is silent. Neither replacement carries a list: the
 * version is computed from the definitions in full, and a secret is digested over the set of fields the
 * configuration itself declares secret.
 */
public class CacheKeyDefinitionVersionTests extends ESTestCase {

    /** A resolver-shaped config: dataset settings at the top, the data source's own under {@code _datasource}. */
    private static Map<String, Object> config(String definitionVersion, Map<String, Object> dataSourceSettings) {
        Map<String, Object> config = new HashMap<>();
        config.put("format", "csv");
        config.put(DefinitionVersion.CONFIG_KEY, definitionVersion);
        config.put("_datasource", new HashMap<>(dataSourceSettings));
        return config;
    }

    /**
     * A data source as production stores one: a secret is an {@link EncryptedData} carrier, never plaintext. The
     * shape matters here — over a plaintext carrier the version folds the token's value and this case separates
     * for a reason that does not hold on a cluster with encryption configured.
     */
    private static DataSource sourceWithToken(String sessionToken) {
        Map<String, DataSourceSetting> settings = new LinkedHashMap<>();
        settings.put("auth", new DataSourceSetting("static_credentials", false));
        settings.put("access_key", new DataSourceSetting(encrypted("AKIAEXAMPLE"), true));
        settings.put("session_token", new DataSourceSetting(encrypted(sessionToken), true));
        return new DataSource("src", "s3", null, settings);
    }

    private static EncryptedData encrypted(String plaintext) {
        return new EncryptedData("project-key-1", plaintext.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * The case from the security report, computed rather than assumed: one bucket, one prefix, two identities
     * differing only in the session token. Every value here is derived — the version by
     * {@link DefinitionVersion#of} and the secret identity by {@link Configured#secretIdentityOf} — so the case
     * fails if either stops distinguishing them, which asserting two literals could not detect.
     * <p>
     * It also pins WHICH of the two separates them. Stored encrypted, as production stores a secret, the token
     * does not reach the version at all: ciphertext carries a fresh IV per write, so folding it would version
     * definitions that had not changed. The separation is the secret identity, digested from the decrypted
     * values the provider is handed. {@code session_token} appears on no hand-written list in either — the
     * version walks the definitions in full, and the digest covers the fields the configuration declares secret.
     */
    public void testTwoIdentitiesOverOneBucketDoNotShareAListing() {
        Dataset dataset = new Dataset("parts", new DataSourceReference("src"), "s3://warehouse/data/*.parquet", null, Map.of());
        String readerVersion = DefinitionVersion.of(dataset, sourceWithToken("READERTOKEN"));
        String auditorVersion = DefinitionVersion.of(dataset, sourceWithToken("AUDITORTOKEN"));
        assertEquals("an encrypted secret must not reach the definition version", readerVersion, auditorVersion);

        Set<String> declaredSecrets = Set.of("access_key", "secret_key", "session_token");
        Map<String, Object> reader = Map.of("auth", "static_credentials", "access_key", "AKIAEXAMPLE", "session_token", "READERTOKEN");
        Map<String, Object> auditor = Map.of("auth", "static_credentials", "access_key", "AKIAEXAMPLE", "session_token", "AUDITORTOKEN");
        assertNotEquals(
            "two identities over one prefix must not address one listing",
            ListingCacheKey.build(
                "s3",
                "warehouse",
                "data/*.parquet",
                "",
                Configured.secretIdentityOf(reader, declaredSecrets),
                config(readerVersion, reader),
                ""
            ),
            ListingCacheKey.build(
                "s3",
                "warehouse",
                "data/*.parquet",
                "",
                Configured.secretIdentityOf(auditor, declaredSecrets),
                config(auditorVersion, auditor),
                ""
            )
        );
    }

    /** The same identity reaching the same prefix must still hit, or the listing cache never warms. */
    public void testOneIdentityOverOneBucketAddressesOneListing() {
        Map<String, Object> settings = Map.of("auth", "static_credentials", "access_key", "AKIAEXAMPLE", "session_token", "TOKEN");
        assertEquals(
            ListingCacheKey.build("s3", "warehouse", "data/*.parquet", "", "", config("v1", settings), ""),
            ListingCacheKey.build("s3", "warehouse", "data/*.parquet", "", "", config("v1", settings), "")
        );
    }

    /** The per-file schema and statistics entries are derived from the definitions too. */
    public void testSchemaKeysDifferAcrossDefinitionVersions() {
        Map<String, Object> settings = Map.of("auth", "anonymous");
        assertNotEquals(
            SchemaCacheKey.build("s3://warehouse/data/a.parquet", 1000L, "parquet", "", config("v1", settings)),
            SchemaCacheKey.build("s3://warehouse/data/a.parquet", 1000L, "parquet", "", config("v2", settings))
        );
        assertEquals(
            SchemaCacheKey.build("s3://warehouse/data/a.parquet", 1000L, "parquet", "", config("v1", settings)),
            SchemaCacheKey.build("s3://warehouse/data/a.parquet", 1000L, "parquet", "", config("v1", settings))
        );
    }

    /**
     * A query that reaches a cache without a registered dataset behind it has no definition to version.
     * Those entries share one value and are addressed as they were before, rather than each becoming
     * unreachable to the next query.
     */
    public void testAnAbsentVersionIsStableRatherThanUnique() {
        Map<String, Object> noVersion = new HashMap<>();
        noVersion.put("format", "csv");
        assertEquals(
            SchemaCacheKey.build("s3://warehouse/data/a.parquet", 1000L, "parquet", "", noVersion),
            SchemaCacheKey.build("s3://warehouse/data/a.parquet", 1000L, "parquet", "", noVersion)
        );
    }

    /**
     * The inline path, which has no dataset and therefore no definition version. Two queries carrying their own
     * credentials over one bucket are separated by nothing else: the storage identity excludes secrets so that two
     * users of one data source share what a file contains, and the definition version is empty here. So the secret
     * identity the provider derives is the only thing between them, and {@code session_token} has to be in it.
     * <p>
     * Before this it was not. The listing key computed its own hash from seven credential names, {@code
     * session_token} was absent from the list, and these two keys were equal.
     */
    public void testTwoInlineIdentitiesOverOneBucketDoNotShareAListing() {
        Set<String> declaredSecrets = Set.of("access_key", "secret_key", "session_token");
        Map<String, Object> reader = Map.of("access_key", "AKIAEXAMPLE", "session_token", "READERTOKEN");
        Map<String, Object> auditor = Map.of("access_key", "AKIAEXAMPLE", "session_token", "AUDITORTOKEN");

        Map<String, Object> inline = new HashMap<>();
        inline.put("format", "csv");
        assertEquals("an inline query carries no definition version", "", SchemaCacheKey.definitionVersionOf(inline));

        assertNotEquals(
            "two inline identities over one prefix must not address one listing",
            ListingCacheKey.build(
                "s3",
                "warehouse",
                "data/*.parquet",
                "",
                Configured.secretIdentityOf(reader, declaredSecrets),
                inline,
                ""
            ),
            ListingCacheKey.build(
                "s3",
                "warehouse",
                "data/*.parquet",
                "",
                Configured.secretIdentityOf(auditor, declaredSecrets),
                inline,
                ""
            )
        );
    }

    /**
     * The other half of the same rule: what a file <i>contains</i> does not depend on who read it, so a credential
     * must not fragment the schema key. Two inline identities over one file share its schema entry and each pays
     * one cold read between them rather than one each.
     */
    public void testTwoInlineIdentitiesOverOneFileShareItsSchema() {
        Map<String, Object> reader = new HashMap<>();
        reader.put("format", "csv");
        Map<String, Object> auditor = new HashMap<>();
        auditor.put("format", "csv");
        assertEquals(
            SchemaCacheKey.build("s3://warehouse/data/a.parquet", 1000L, "parquet", "", reader),
            SchemaCacheKey.build("s3://warehouse/data/a.parquet", 1000L, "parquet", "", auditor)
        );
    }

}
