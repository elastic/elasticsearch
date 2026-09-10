/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.AbstractColumnarMapperCompatibilityTestCase;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class VersionStringFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "f";

    @Override
    protected Collection<Plugin> getPlugins() {
        return List.of(new VersionFieldPlugin(Settings.EMPTY));
    }

    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    public void testSingleValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "version").endObject()),
            columnarSettings(),
            batch("single value", 1L, doc("d1", 1L, "{\"f\":\"1.2.3\"}"))
        );
    }

    public void testAllPresentDense() throws IOException {
        // Every doc has a value; exercises the DENSE (validity==null) column wrap.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "version").endObject()),
            columnarSettings(),
            batch(
                "all present dense",
                1L,
                doc("d1", 1L, "{\"f\":\"1.0.0\"}"),
                doc("d2", 2L, "{\"f\":\"2.0.0\"}"),
                doc("d3", 3L, "{\"f\":\"3.0.0\"}")
            )
        );
    }

    public void testAbsentDoc() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "version").endObject()),
            columnarSettings(),
            batch("absent doc", 1L, doc("d1", 1L, "{}"))
        );
    }

    public void testMixedAbsentPresent() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "version").endObject()),
            columnarSettings(),
            batch("mixed absent present", 1L, doc("d1", 1L, "{\"f\":\"1.0.0\"}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":\"2.0.0\"}"))
        );
    }

    public void testExplicitNull() throws IOException {
        // Explicit JSON null and absent doc both produce no fields; version has no null_value parameter.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "version").endObject()),
            columnarSettings(),
            batch("explicit null", 1L, doc("d1", 1L, "{\"f\":null}"), doc("d2", 2L, "{}"))
        );
    }

    // -------------------------------------------------------------------------
    // Multi-valued tests
    // -------------------------------------------------------------------------

    public void testMultiValueArray() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "version").endObject()),
            columnarSettings(),
            batch("multi-value array", 1L, doc("d1", 1L, "{\"f\":[\"1.0.0\",\"2.0.0\"]}"))
        );
    }

    public void testArrayValues() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "version").endObject()),
            columnarSettings(),
            batch(
                "array values",
                1L,
                doc("d1", 1L, "{\"f\":[\"1.0.0\"]}"),
                doc("d2", 2L, "{\"f\":[\"2.0.0\",\"3.0.0\",\"4.0.0\"]}"),
                doc("d3", 3L, "{\"f\":[]}"),
                doc("d4", 4L, "{}")
            )
        );
    }

    public void testArrayContainingNull() throws IOException {
        // Nulls in an array are silently ignored (no null_value substitution).
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "version").endObject()),
            columnarSettings(),
            batch("array containing null", 1L, doc("d1", 1L, "{\"f\":[\"1.0.0\",null,\"2.0.0\"]}"), doc("d2", 2L, "{}"))
        );
    }

    public void testDuplicateValuesInArray() throws IOException {
        // The SORTED_SET doc-values writer deduplicates ordinals; the inverted-index writer deduplicates
        // terms. Both the row and columnar paths rely on Lucene to handle deduplication identically.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "version").endObject()),
            columnarSettings(),
            batch("duplicate values in array", 1L, doc("d1", 1L, "{\"f\":[\"1.0.0\",\"1.0.0\",\"2.0.0\"]}"))
        );
    }

    public void testNestedArrayFlattening() throws IOException {
        // Nested arrays are flattened, matching the row-path behaviour in DocumentParser.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "version").endObject()),
            columnarSettings(),
            batch("nested array flattening", 1L, doc("d1", 1L, "{\"f\":[[\"1.0.0\",\"2.0.0\"],[\"3.0.0\"]]}"), doc("d2", 2L, "{}"))
        );
    }

    // -------------------------------------------------------------------------
    // Encoder coverage — version-specific variants
    // -------------------------------------------------------------------------

    public void testPreRelease() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "version").endObject()),
            columnarSettings(),
            batch(
                "pre-release versions",
                1L,
                doc("d1", 1L, "{\"f\":\"1.0.0-alpha\"}"),
                doc("d2", 2L, "{\"f\":\"1.0.0-alpha.1\"}"),
                doc("d3", 3L, "{\"f\":\"1.0.0-0.3.7\"}")
            )
        );
    }

    public void testBuildSuffix() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "version").endObject()),
            columnarSettings(),
            batch("build suffix versions", 1L, doc("d1", 1L, "{\"f\":\"1.0.0+build.1\"}"), doc("d2", 2L, "{\"f\":\"1.0.0-alpha+001\"}"))
        );
    }

    public void testIllegalVersionStrings() throws IOException {
        // Non-semver strings hit the isLegal==false path, which encodes the raw string bytes.
        // This verifies that the columnar encoder produces the same raw-bytes result as the row path.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "version").endObject()),
            columnarSettings(),
            batch(
                "illegal version strings",
                1L,
                doc("d1", 1L, "{\"f\":\"not-a-version\"}"),
                doc("d2", 2L, "{\"f\":\"1.2.3.4.5-SNAPSHOT\"}"),
                doc("d3", 3L, "{\"f\":\"v1.0\"}")
            )
        );
    }

    public void testEmptyString() throws IOException {
        // Empty string exercises VersionEncoder's ENCODED_EMPTY_STRING special case.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "version").endObject()),
            columnarSettings(),
            batch("empty string", 1L, doc("d1", 1L, "{\"f\":\"\"}"))
        );
    }

    public void testLongDigitGroups() throws IOException {
        // A multi-digit numeric group exercises the NUMERIC_MARKER_BYTE + length prefix encoding in
        // prefixDigitGroupsWithLength. The group must fit in Integer.MAX_VALUE (VersionEncoder parses
        // numeric identifiers with Integer.valueOf), so we use a 9-digit number safely below 2^31-1.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "version").endObject()),
            columnarSettings(),
            batch("long digit groups", 1L, doc("d1", 1L, "{\"f\":\"1.0.999999999\"}"))
        );
    }

    public void testManyIdentifiers() throws IOException {
        // More than MAX_LEGAL_VERSION_IDENTIFIERS (32) dot-separated identifiers triggers the
        // tooManyIdentifiers() path, which falls back to raw-string encoding (isLegal==false).
        String manyDots = "1." + "0.".repeat(33) + "0";
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "version").endObject()),
            columnarSettings(),
            batch("many identifiers", 1L, doc("d1", 1L, "{\"f\":\"" + manyDots + "\"}"))
        );
    }

    public void testNonAscii() throws IOException {
        // Multi-byte UTF-8 string hits the isLegal==false path; verifies the utf8ToString()
        // round-trip preserves the original bytes.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "version").endObject()),
            columnarSettings(),
            batch("non-ascii version string", 1L, doc("d1", 1L, "{\"f\":\"1.0.0-é\"}"))
        );
    }

    public void testLargeMixedBatch() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "version").endObject()),
            columnarSettings(),
            batch(
                "large mixed batch",
                1L,
                doc("d1", 1L, "{\"f\":\"1.0.0\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":[\"2.0.0\",\"3.0.0-beta\"]}"),
                doc("d4", 4L, "{\"f\":\"4.0.0+build.1\"}"),
                doc("d5", 5L, "{}"),
                doc("d6", 6L, "{\"f\":\"not-a-version\"}"),
                doc("d7", 7L, "{}")
            )
        );
    }

    /**
     * Verifies that a canonical integer literal in the source (e.g. {@code {"f": 1}}) is stringified
     * correctly to {@code "1"} on the columnar path and produces the same term as the row path.
     */
    public void testNumericSourceValue() throws IOException {
        // {"f": 1} -> utf8Cursor yields "1" (canonical toString), encodeVersion("1") produces
        // the same bytes as encodeVersion(parser.getText()) on the row path. This test exercises
        // the number-to-string conversion for a canonical literal.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "version").endObject()),
            columnarSettings(),
            batch("numeric source value canonical", 1L, doc("d1", 1L, "{\"f\":1}"))
        );
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-team/issues/4920")
    public void testNonCanonicalNumericLiteral() throws IOException {
        // {"f": 1.50}: row path -> parser.getText() = "1.50"; columnar path -> utf8Cursor = "1.5".
        // These produce different encoded terms, causing a parity failure.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "version").endObject()),
            columnarSettings(),
            batch("non-canonical numeric literal", 1L, doc("d1", 1L, "{\"f\":1.50}"), doc("d2", 2L, "{\"f\":\"2.0.0\"}"))
        );
    }

    private static final BytesRef TS_TSID = new BytesRef(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 });
    private static final int TS_ROUTING_HASH = 42;
    private static final String TS_ROUTING = TimeSeriesRoutingHashFieldMapper.encode(TS_ROUTING_HASH);
    // epoch millis: 2024-01-15T12:00:00.000Z, 2024-06-01T00:00:00.000Z
    private static final long TS_A = 1705320000000L;
    private static final long TS_B = 1717200000000L;

    private static Settings tsdbSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .putList(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .put(IndexSettings.SYNTHETIC_ID.getKey(), false)
            .build();
    }

    public void testTsdbSingleValue() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(TS_ROUTING_HASH, TS_TSID, TS_A);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "version").endObject();
        }),
            tsdbSettings(),
            batch("tsdb single value", 1L, doc(idA, TS_ROUTING, TS_TSID, 1L, "{\"@timestamp\":" + TS_A + ",\"f\":\"1.2.3\"}"))
        );
    }

    public void testTsdbAbsentAndMultiValue() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(TS_ROUTING_HASH, TS_TSID, TS_A);
        final String idB = TsidExtractingIdFieldMapper.createId(TS_ROUTING_HASH, TS_TSID, TS_B);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "version").endObject();
        }),
            tsdbSettings(),
            batch(
                "tsdb absent and multi-value",
                1L,
                doc(idA, TS_ROUTING, TS_TSID, 1L, "{\"@timestamp\":" + TS_A + ",\"f\":[\"2.0.0\",\"1.0.0-alpha\"]}"),
                doc(idB, TS_ROUTING, TS_TSID, 2L, "{\"@timestamp\":" + TS_B + "}")
            )
        );
    }
}
