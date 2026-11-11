/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceTelemetryVocabulary.Type;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class DataSourceTelemetryVocabularyTests extends ESTestCase {

    public void testFromTypeIdKnownIds() {
        assertSame(Type.S3, Type.fromTypeId("s3"));
        assertSame(Type.GCS, Type.fromTypeId("gcs"));
        assertSame(Type.AZURE, Type.fromTypeId("azure"));
        assertSame(Type.HTTP, Type.fromTypeId("http"));
        assertSame(Type.LOCAL, Type.fromTypeId("local"));
        assertSame(Type.UNKNOWN, Type.fromTypeId("unknown"));
        assertSame(Type.S3, Type.fromTypeId("S3"));
    }

    public void testFromTypeIdDoesNotFoldSchemeAliases() {
        assertSame(Type.UNKNOWN, Type.fromTypeId("file"));
        assertSame(Type.UNKNOWN, Type.fromTypeId("s3a"));
        assertSame(Type.UNKNOWN, Type.fromTypeId("s3n"));
        assertSame(Type.UNKNOWN, Type.fromTypeId("gs"));
        assertSame(Type.UNKNOWN, Type.fromTypeId("wasb"));
        assertSame(Type.UNKNOWN, Type.fromTypeId("wasbs"));
        assertSame(Type.UNKNOWN, Type.fromTypeId("https"));
        assertSame(Type.UNKNOWN, Type.fromTypeId("ftp"));
        assertSame(Type.UNKNOWN, Type.fromTypeId(null));
        assertSame(Type.UNKNOWN, Type.fromTypeId(""));
    }

    public void testFromSchemeFoldsAliasesAndFileToLocal() {
        assertSame(Type.S3, Type.fromScheme("s3"));
        assertSame(Type.S3, Type.fromScheme("s3a"));
        assertSame(Type.S3, Type.fromScheme("s3n"));
        assertSame(Type.S3, Type.fromScheme("S3A"));
        assertSame(Type.GCS, Type.fromScheme("gs"));
        assertSame(Type.GCS, Type.fromScheme("gcs"));
        assertSame(Type.AZURE, Type.fromScheme("wasb"));
        assertSame(Type.AZURE, Type.fromScheme("wasbs"));
        assertSame(Type.AZURE, Type.fromScheme("azure"));
        assertSame(Type.HTTP, Type.fromScheme("http"));
        assertSame(Type.HTTP, Type.fromScheme("https"));
        assertSame(Type.LOCAL, Type.fromScheme("file"));
        assertSame(Type.LOCAL, Type.fromScheme("FILE"));
    }

    public void testFromSchemeClampsUnknown() {
        assertSame(Type.UNKNOWN, Type.fromScheme("ftp"));
        assertSame(Type.UNKNOWN, Type.fromScheme("local"));
        assertSame(Type.UNKNOWN, Type.fromScheme(null));
        assertSame(Type.UNKNOWN, Type.fromScheme(""));
    }

    /**
     * For every plugin-registered validator type {@code t} with declared scheme {@code s},
     * {@link Type#fromScheme(String) fromScheme(s)} equals {@link Type#fromTypeId(String) fromTypeId(t)}.
     * This is the regression pin for folding {@code file} → {@code local}.
     * <p>
     * Cloud plugins are not on this unit-test classpath (their SDKs jar-hell with esql tests), so the
     * type/scheme pairs here are the {@code FileDataSourceValidator} constructor arguments from each
     * plugin. S3, GCS and Azure pin this against the live plugin in their own tests.
     * {@link HttpDataSourcePlugin} is on the classpath and is asserted live below.
     */
    public void testSchemeFoldAgreesWithTypeId() {
        List<FileDataSourceValidator> validators = List.of(
            new FileDataSourceValidator("s3", (raw, consumed) -> null, Set.of("s3", "s3a", "s3n")),
            new FileDataSourceValidator("gcs", (raw, consumed) -> null, Set.of("gs")),
            new FileDataSourceValidator("azure", (raw, consumed) -> null, Set.of("wasbs", "wasb")),
            new FileDataSourceValidator("http", (raw, consumed) -> null, Set.of("http", "https")),
            new FileDataSourceValidator("local", (raw, consumed) -> null, Set.of("file"))
        );
        Set<String> seen = new HashSet<>();
        for (FileDataSourceValidator validator : validators) {
            assertSchemeFoldAgreesWithTypeId(validator, seen);
        }
        assertThat(seen, equalTo(expectedKnownTypeIds()));

        for (DataSourceValidator validator : new HttpDataSourcePlugin().datasourceValidators(Settings.EMPTY).values()) {
            if (validator instanceof FileDataSourceValidator fileValidator) {
                assertSchemeFoldAgreesWithTypeId(fileValidator, new HashSet<>());
            } else {
                fail("expected FileDataSourceValidator for type [" + validator.type() + "], got " + validator.getClass().getName());
            }
        }
    }

    private static Set<String> expectedKnownTypeIds() {
        Set<String> expected = new HashSet<>();
        for (Type type : Type.values()) {
            if (type != Type.UNKNOWN) {
                expected.add(type.key());
            }
        }
        return expected;
    }

    private static void assertSchemeFoldAgreesWithTypeId(FileDataSourceValidator validator, Set<String> seen) {
        String typeId = validator.type();
        seen.add(typeId);
        Type fromId = Type.fromTypeId(typeId);
        for (String scheme : validator.supportedSchemes()) {
            assertSame("type [" + typeId + "] scheme [" + scheme + "]", fromId, Type.fromScheme(scheme));
        }
    }
}
