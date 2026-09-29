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
import org.elasticsearch.xpack.esql.datasources.DefinitionVersion;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

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
 * setting has to be remembered too, and forgetting is silent. A version computed from the definitions in
 * full has no list to omit a name from.
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

    /** A data source as stored, with its secrets marked secret. */
    private static DataSource sourceWithToken(String sessionToken) {
        Map<String, DataSourceSetting> settings = new LinkedHashMap<>();
        settings.put("auth", new DataSourceSetting("static_credentials", false));
        settings.put("access_key", new DataSourceSetting("AKIAEXAMPLE", true));
        settings.put("session_token", new DataSourceSetting(sessionToken, true));
        return new DataSource("src", "s3", null, settings);
    }

    /**
     * The case from the security report, computed rather than assumed: one bucket, one prefix, two
     * identities differing only in the session token. The version is derived from each definition by
     * {@link DefinitionVersion#of}, so this fails if that value stops distinguishing them — which
     * asserting two different literals could not detect.
     * <p>
     * {@code session_token} appears on no list anywhere. That is the point: a version computed from the
     * definitions in full has no list to omit a name from.
     */
    public void testTwoIdentitiesOverOneBucketDoNotShareAListing() {
        Dataset dataset = new Dataset("parts", new DataSourceReference("src"), "s3://warehouse/data/*.parquet", null, Map.of());
        String readerVersion = DefinitionVersion.of(dataset, sourceWithToken("READERTOKEN"));
        String auditorVersion = DefinitionVersion.of(dataset, sourceWithToken("AUDITORTOKEN"));
        assertNotEquals("two identities must not compute one definition version", readerVersion, auditorVersion);

        Map<String, Object> reader = Map.of("auth", "static_credentials", "access_key", "AKIAEXAMPLE", "session_token", "READERTOKEN");
        Map<String, Object> auditor = Map.of("auth", "static_credentials", "access_key", "AKIAEXAMPLE", "session_token", "AUDITORTOKEN");
        assertNotEquals(
            "two identities over one prefix must not address one listing",
            ListingCacheKey.build("s3", "warehouse", "data/*.parquet", "", config(readerVersion, reader), ""),
            ListingCacheKey.build("s3", "warehouse", "data/*.parquet", "", config(auditorVersion, auditor), "")
        );
    }

    /** The same identity reaching the same prefix must still hit, or the listing cache never warms. */
    public void testOneIdentityOverOneBucketAddressesOneListing() {
        Map<String, Object> settings = Map.of("auth", "static_credentials", "access_key", "AKIAEXAMPLE", "session_token", "TOKEN");
        assertEquals(
            ListingCacheKey.build("s3", "warehouse", "data/*.parquet", "", config("v1", settings), ""),
            ListingCacheKey.build("s3", "warehouse", "data/*.parquet", "", config("v1", settings), "")
        );
    }

    /** The per-file schema and statistics entries are derived from the definitions too. */
    public void testSchemaKeysDifferAcrossDefinitionVersions() {
        Map<String, Object> settings = Map.of("auth", "anonymous");
        assertNotEquals(
            SchemaCacheKey.build("s3://warehouse/data/a.parquet", 1000L, "parquet", config("v1", settings)),
            SchemaCacheKey.build("s3://warehouse/data/a.parquet", 1000L, "parquet", config("v2", settings))
        );
        assertEquals(
            SchemaCacheKey.build("s3://warehouse/data/a.parquet", 1000L, "parquet", config("v1", settings)),
            SchemaCacheKey.build("s3://warehouse/data/a.parquet", 1000L, "parquet", config("v1", settings))
        );
    }

    /** So is the file metadata entry. */
    public void testFileMetadataKeysDifferAcrossDefinitionVersions() {
        Map<String, Object> settings = Map.of("auth", "anonymous");
        assertNotEquals(
            FileMetadataCacheKey.build("s3://warehouse/data/a.parquet", "", config("v1", settings)),
            FileMetadataCacheKey.build("s3://warehouse/data/a.parquet", "", config("v2", settings))
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
            SchemaCacheKey.build("s3://warehouse/data/a.parquet", 1000L, "parquet", noVersion),
            SchemaCacheKey.build("s3://warehouse/data/a.parquet", 1000L, "parquet", noVersion)
        );
    }
}
