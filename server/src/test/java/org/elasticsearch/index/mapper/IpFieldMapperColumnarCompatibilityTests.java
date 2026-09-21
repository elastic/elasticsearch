/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.recovery.RecoverySettings;

import java.io.IOException;
import java.util.List;

public class IpFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "f";

    /**
     * Extends the base equality check with:
     * <ul>
     *   <li>An explicit doc-values-type assertion for {@code FIELD}: when {@code expected} produces a
     *       non-NONE doc-values type, this asserts that {@code actual} matches it as a named assertion
     *       rather than buried in the full set diff.</li>
     *   <li>Exclusion of {@code _id} from comparison in TSDB scenarios (detected by the presence of
     *       {@code _tsid} in {@code expected}): the columnar path splits {@code _id} into a DV column
     *       and a separate TokenStreamColumn while the row path combines them — a known divergence
     *       covered by {@link TsidExtractingIdFieldMapperColumnarCompatibilityTests}.</li>
     * </ul>
     */
    @Override
    protected void assertFieldSetsEqual(List<FieldDescriptor> expected, List<FieldDescriptor> actual, String message) {
        for (FieldDescriptor fd : expected) {
            if (fd.name().equals(FIELD) && fd.fieldType().docValuesType() != DocValuesType.NONE) {
                final DocValuesType dvType = fd.fieldType().docValuesType();
                assertTrue(
                    message + ": field [" + FIELD + "] expected docValuesType=" + dvType + " but columnar path did not produce it",
                    actual.stream().anyMatch(a -> a.name().equals(FIELD) && a.fieldType().docValuesType() == dvType)
                );
            }
        }
        final boolean isTsdb = expected.stream().anyMatch(fd -> fd.name().equals("_tsid"));
        if (isTsdb) {
            expected = expected.stream().filter(fd -> fd.name().equals("_id") == false).toList();
            actual = actual.stream().filter(fd -> fd.name().equals("_id") == false).toList();
        }
        super.assertFieldSetsEqual(expected, actual, message);
    }

    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    public void testSingleValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch("single value", 1L, doc("d1", 1L, "{\"f\":\"192.168.0.1\"}"))
        );
    }

    public void testSingleValueIpv6() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch("single value ipv6", 1L, doc("d1", 1L, "{\"f\":\"2001:db8::1\"}"))
        );
    }

    public void testAbsentDoc() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch("absent doc", 1L, doc("d1", 1L, "{}"))
        );
    }

    public void testMixedAbsentPresent() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch(
                "mixed absent present",
                1L,
                doc("d1", 1L, "{\"f\":\"10.0.0.1\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":\"10.0.0.3\"}")
            )
        );
    }

    public void testMultiValueArray() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch("multi-value array", 1L, doc("d1", 1L, "{\"f\":[\"10.0.0.1\",\"10.0.0.2\"]}"))
        );
    }

    public void testArrayValues() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch(
                "array values",
                1L,
                doc("d1", 1L, "{\"f\":[\"10.0.0.1\"]}"),
                doc("d2", 2L, "{\"f\":[\"10.0.0.2\",\"10.0.0.3\",\"10.0.0.4\"]}"),
                doc("d3", 3L, "{\"f\":[]}"),
                doc("d4", 4L, "{}")
            )
        );
    }

    public void testExplicitNullNoNullValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch("explicit null no null_value", 1L, doc("d1", 1L, "{\"f\":null}"), doc("d2", 2L, "{}"))
        );
    }

    public void testNullValueSubstitution() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").field("null_value", "0.0.0.0").endObject()),
            columnarSettings(),
            batch("null_value substitution", 1L, doc("d1", 1L, "{\"f\":null}"), doc("d2", 2L, "{\"f\":\"1.2.3.4\"}"), doc("d3", 3L, "{}"))
        );
    }

    public void testArrayContainingNull() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch("array containing null", 1L, doc("d1", 1L, "{\"f\":[\"10.0.0.1\",null,\"10.0.0.2\"]}"), doc("d2", 2L, "{}"))
        );
    }

    public void testMixedIpv4Ipv6() throws IOException {
        // IPv4 and IPv6 addresses in the same batch. An IPv4 address stored as IPv4-mapped IPv6 (e.g.
        // ::ffff:192.168.0.1) should encode identically to the plain IPv4 address 192.168.0.1.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch(
                "mixed ipv4 ipv6",
                1L,
                doc("d1", 1L, "{\"f\":\"192.168.0.1\"}"),
                doc("d2", 2L, "{\"f\":\"2001:db8::1\"}"),
                doc("d3", 3L, "{\"f\":\"::ffff:192.168.0.1\"}")
            )
        );
    }

    public void testDuplicateValuesInArray() throws IOException {
        // Array-order path preserves duplicates (unlike SORTED_UNIQUE).
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch("duplicate values", 1L, doc("d1", 1L, "{\"f\":[\"10.0.0.1\",\"10.0.0.1\",\"10.0.0.2\"]}"))
        );
    }

    public void testNestedArrayFlattening() throws IOException {
        // Nested arrays are flattened, matching the row-path behaviour in DocumentParser.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch("nested array flattening", 1L, doc("d1", 1L, "{\"f\":[[\"10.0.0.1\",\"10.0.0.2\"],[\"10.0.0.3\"]]}"), doc("d2", 2L, "{}"))
        );
    }

    public void testLargeMixedBatch() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").endObject()),
            columnarSettings(),
            batch(
                "large mixed batch",
                1L,
                doc("d1", 1L, "{\"f\":\"1.1.1.1\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":[\"2.2.2.2\",\"3.3.3.3\"]}"),
                doc("d4", 4L, "{\"f\":\"4.4.4.4\"}"),
                doc("d5", 5L, "{}"),
                doc("d6", 6L, "{\"f\":\"2001:db8::cafe\"}"),
                doc("d7", 7L, "{}")
            )
        );
    }

    public void testSingleValueMultiValueFalse() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "ip");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "single value multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":\"10.0.0.1\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":\"10.0.0.3\"}")
            )
        );
    }

    public void testAbsentAndNullMultiValueFalse() throws IOException {
        // Present value, absent doc ({}), and explicit JSON null without null_value -> absent.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "ip");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "absent and null multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":\"10.0.0.1\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":null}")
            )
        );
    }

    public void testNullValueSubstitutionMultiValueFalse() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "ip").field("null_value", "0.0.0.0");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "null_value substitution multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":null}"),
                doc("d2", 2L, "{\"f\":\"1.2.3.4\"}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    public void testSingleElementArrayMultiValueFalse() throws IOException {
        // A single-element array {"f":["1.1.1.1"]} is a legal value for a multi_value=false field.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "ip");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "single element array multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":[\"1.1.1.1\"]}"),
                doc("d2", 2L, "{\"f\":[]}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    public void testAllPresentDenseMultiValueFalse() throws IOException {
        // Every doc has an ip value; no absent docs. Exercises the dense (validity==null) wrap.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "ip");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "all present dense multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":\"10.0.0.1\"}"),
                doc("d2", 2L, "{\"f\":\"10.0.0.2\"}"),
                doc("d3", 3L, "{\"f\":\"10.0.0.3\"}")
            )
        );
    }

    public void testManyMixedPresentAbsentMultiValueFalse() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "ip");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "many mixed present absent multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":\"1.1.1.1\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":\"3.3.3.3\"}"),
                doc("d4", 4L, "{}"),
                doc("d5", 5L, "{\"f\":\"5.5.5.5\"}"),
                doc("d6", 6L, "{\"f\":\"6.6.6.6\"}"),
                doc("d7", 7L, "{}")
            )
        );
    }

    public void testIpv6MultiValueFalse() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "ip");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }), columnarSettings(), batch("ipv6 multi_value=false", 1L, doc("d1", 1L, "{\"f\":\"2001:db8::1\"}"), doc("d2", 2L, "{}")));
    }

    @AwaitsFix(
        bugUrl = "columnar mapColumnBatch does not implement ignore_malformed for ip fields; malformed values fall back to the row path"
    )
    public void testIgnoreMalformed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").field("ignore_malformed", true).endObject()),
            columnarSettings(),
            batch("ignore_malformed", 1L, doc("d1", 1L, "{\"f\":\"not-an-ip\"}"), doc("d2", 2L, "{\"f\":\"10.0.0.1\"}"))
        );
    }

    public void testColumnarDimensionSingleValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").field("time_series_dimension", true).endObject()),
            columnarSettings(),
            batch("columnar ip dimension single value", 1L, doc("d1", 1L, "{\"f\":\"10.0.0.1\"}"))
        );
    }

    public void testColumnarDimensionAbsentDoc() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").field("time_series_dimension", true).endObject()),
            columnarSettings(),
            batch(
                "columnar ip dimension absent doc",
                1L,
                doc("d1", 1L, "{\"f\":\"10.0.0.1\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":\"10.0.0.3\"}")
            )
        );
    }

    public void testColumnarDimensionIpv4Ipv6Mix() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "ip").field("time_series_dimension", true).endObject()),
            columnarSettings(),
            batch(
                "columnar ip dimension ipv4/ipv6 mix",
                1L,
                doc("d1", 1L, "{\"f\":\"192.168.0.1\"}"),
                doc("d2", 2L, "{\"f\":\"2001:db8::1\"}"),
                doc("d3", 3L, "{\"f\":\"::ffff:192.168.0.1\"}")
            )
        );
    }

    public void testDimensionRoutingPathIsNotColumnar() throws IOException {
        // index.routing_path resolves to ForRoutingPath, whose extractDimensionsWhileMapping() is true, so
        // the row path writes the dimension to the routing fields and the columnar path must refuse.
        final MapperService mapperService = createMapperService(tsdbSettings(IndexMetadata.INDEX_ROUTING_PATH.getKey()), mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject(FIELD).field("type", "ip").field("time_series_dimension", true).endObject();
        }));
        final FieldMapper mapper = (FieldMapper) mapperService.mappingLookup().getMapper(FIELD);
        assertFalse(
            "a dimension whose routing is extracted while mapping must not be columnar",
            mapper.supportsColumnarParse(mapperService.getIndexSettings())
        );
    }

    public void testTsdbDimensionIsColumnar() throws IOException {
        // TSDB ip dimensions resolve to IndexType.skippers() — SORTED_SET doc values with a RANGE skip
        // index. The columnar batch path now emits native SORTED_SET doc values, so these fields take
        // the columnar path. Routing-path dimensions are still excluded (testDimensionRoutingPathIsNotColumnar).
        final MapperService mapperService = createMapperService(tsdbSettings(IndexMetadata.INDEX_DIMENSIONS.getKey()), mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject(FIELD).field("type", "ip").field("time_series_dimension", true).endObject();
        }));
        final FieldMapper mapper = (FieldMapper) mapperService.mappingLookup().getMapper(FIELD);
        assertTrue(
            "precondition: TSDB ip dimension must use a SORTED_SET doc-values skipper",
            mapper.fieldType().indexType().hasDocValuesSkipper()
        );
        assertTrue(
            "TSDB ip dimensions now take the columnar path via SORTED_SET emission",
            mapper.supportsColumnarParse(mapperService.getIndexSettings())
        );
    }

    /** TIME_SERIES settings listing {@code f} under the given dimension-source setting. */
    private static Settings tsdbSettings(String dimensionSettingKey) {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .putList(dimensionSettingKey, FIELD)
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
            .build();
    }

    public void testMultiValueViolationBailsOutOfColumnarPath() throws IOException {
        // Two values for a multi_value=false field: mapColumnBatch must throw so that
        // ShardBatchMapper falls back to the row path, which raises the correct
        // on_failure=FAIL document-level error instead.
        final var mapperService = createMapperService(columnarSettings(), mapping(b -> {
            b.startObject(FIELD).field("type", "ip");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }));
        expectThrows(UnsupportedOperationException.class, () -> mapColumnarLeaf(mapperService, FIELD, "{\"f\":[\"1.1.1.1\",\"2.2.2.2\"]}"));
    }

    public void testNullabilityViolationBailsOutOfColumnarPath() throws IOException {
        // A null value for a nullability=false field: mapColumnBatch must throw so that
        // ShardBatchMapper falls back to the row path, which raises the correct
        // on_failure=FAIL document-level error instead.
        final var mapperService = createMapperService(columnarSettings(), mapping(b -> {
            b.startObject(FIELD).field("type", "ip");
            b.startObject("doc_values").field("nullability", false).endObject();
            b.endObject();
        }));
        expectThrows(
            UnsupportedOperationException.class,
            () -> mapColumnarLeaf(mapperService, FIELD, "{\"f\":\"1.1.1.1\"}", "{\"f\":null}")
        );
    }

    public void testIpParentWithKeywordSubField() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "ip");
            b.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "ip parent, keyword sub-field",
                1L,
                doc("d1", 1L, "{\"f\":\"192.168.0.1\"}"),
                doc("d2", 2L, "{\"f\":[\"10.0.0.1\",\"10.0.0.2\"]}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    // =========================================================================
    // TSDB / SORTED_SET: ip fields in TIME_SERIES index mode
    //
    // TSDB ip dimensions resolve to IndexType.skippers() — SORTED_SET doc values with a RANGE skip
    // index. The columnar batch path now emits SORTED_SET directly via LuceneBinaryColumn.of with the
    // sentinel-derived SORTED_SET_DV_SKIPPER_FIELD_TYPE constant.
    // =========================================================================

    private static final BytesRef IP_TSID = new BytesRef(new byte[] { 0x20, 0x30, 0x40, 0x50, 0x60 });
    private static final int IP_ROUTING_HASH = 23;
    private static final String IP_ROUTING = TimeSeriesRoutingHashFieldMapper.encode(IP_ROUTING_HASH);
    // epoch millis: 2025-06-01T00:00:00.000Z
    private static final long IP_TS_A = 1748736000000L;

    /**
     * TIME_SERIES settings with {@code f} (ip) as the only dimension.
     */
    private static Settings tsdbIpSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .putList(IndexMetadata.INDEX_DIMENSIONS.getKey(), FIELD)
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
            .build();
    }

    private static String ipTsdbId(long tsMillis) {
        return TsidExtractingIdFieldMapper.createSyntheticId(IP_TSID, tsMillis, IP_ROUTING_HASH);
    }

    public void testTsdbSingleValue() throws IOException {
        // One IP value per document: scalar STRING column, zero-copy applies, SORTED_SET field type.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject(FIELD).field("type", "ip").field("time_series_dimension", true).endObject();
        }),
            tsdbIpSettings(),
            batch(
                "TSDB ip single value",
                1L,
                doc(ipTsdbId(IP_TS_A), IP_ROUTING, IP_TSID, 1L, "{\"f\":\"192.168.0.1\",\"@timestamp\":" + IP_TS_A + "}"),
                doc(ipTsdbId(IP_TS_A + 1000L), IP_ROUTING, IP_TSID, 2L, "{\"f\":\"10.0.0.1\",\"@timestamp\":" + (IP_TS_A + 1000L) + "}"),
                doc(ipTsdbId(IP_TS_A + 2000L), IP_ROUTING, IP_TSID, 3L, "{\"@timestamp\":" + (IP_TS_A + 2000L) + "}")
            )
        );
    }

    public void testTsdbSingleValueIpv6() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject(FIELD).field("type", "ip").field("time_series_dimension", true).endObject();
        }),
            tsdbIpSettings(),
            batch(
                "TSDB ip single value IPv6",
                1L,
                doc(ipTsdbId(IP_TS_A), IP_ROUTING, IP_TSID, 1L, "{\"f\":\"2001:db8::1\",\"@timestamp\":" + IP_TS_A + "}"),
                doc(ipTsdbId(IP_TS_A + 1000L), IP_ROUTING, IP_TSID, 2L, "{\"f\":\"::1\",\"@timestamp\":" + (IP_TS_A + 1000L) + "}")
            )
        );
    }

    public void testTsdbArrayValues() throws IOException {
        // Multi-valued document: SORTED_SET receives one value per element, deduplicates per doc.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject(FIELD).field("type", "ip").field("time_series_dimension", true).endObject();
        }),
            tsdbIpSettings(),
            batch(
                "TSDB ip array values",
                1L,
                doc(ipTsdbId(IP_TS_A), IP_ROUTING, IP_TSID, 1L, "{\"f\":[\"192.168.0.1\",\"192.168.0.2\"],\"@timestamp\":" + IP_TS_A + "}"),
                doc(ipTsdbId(IP_TS_A + 1000L), IP_ROUTING, IP_TSID, 2L, "{\"f\":\"10.0.0.1\",\"@timestamp\":" + (IP_TS_A + 1000L) + "}"),
                doc(ipTsdbId(IP_TS_A + 2000L), IP_ROUTING, IP_TSID, 3L, "{\"@timestamp\":" + (IP_TS_A + 2000L) + "}")
            )
        );
    }

    public void testTsdbNullValue() throws IOException {
        // null_value substitution: explicit null becomes the configured null_value on both paths.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject(FIELD).field("type", "ip").field("time_series_dimension", true).field("null_value", "0.0.0.0").endObject();
        }),
            tsdbIpSettings(),
            batch(
                "TSDB ip null_value",
                1L,
                doc(ipTsdbId(IP_TS_A), IP_ROUTING, IP_TSID, 1L, "{\"f\":null,\"@timestamp\":" + IP_TS_A + "}"),
                doc(ipTsdbId(IP_TS_A + 1000L), IP_ROUTING, IP_TSID, 2L, "{\"f\":\"192.168.1.1\",\"@timestamp\":" + (IP_TS_A + 1000L) + "}")
            )
        );
    }

    public void testNullValueMultiValueFalseRejectsTwoValues() throws IOException {
        // Pins the pre-existing null_value bug fix: ["192.168.0.1", null] on a multi_value=false
        // ip field with a null_value configured used to write two values instead of bailing.
        // The multi-value check now runs before null_value substitution.
        final MapperService mapperService = createMapperService(columnarSettings(), mapping(b -> {
            b.startObject(FIELD)
                .field("type", "ip")
                .startObject("doc_values")
                .field("multi_value", false)
                .endObject()
                .field("null_value", "0.0.0.0")
                .endObject();
        }));
        expectThrows(UnsupportedOperationException.class, () -> mapColumnarLeaf(mapperService, FIELD, "{\"f\":[\"192.168.0.1\",null]}"));
    }

    public void testNullValueMultiValueFalseNullFirstRejectsTwoValues() throws IOException {
        // Regression: [null, "192.168.0.1"] on a multi_value=false field with null_value must also
        // be rejected. The null element is substituted (first write), so the subsequent real IP is a
        // second value and must trigger the multi_value=false violation.
        final MapperService mapperService = createMapperService(columnarSettings(), mapping(b -> {
            b.startObject(FIELD)
                .field("type", "ip")
                .startObject("doc_values")
                .field("multi_value", false)
                .endObject()
                .field("null_value", "0.0.0.0")
                .endObject();
        }));
        expectThrows(UnsupportedOperationException.class, () -> mapColumnarLeaf(mapperService, FIELD, "{\"f\":[null,\"192.168.0.1\"]}"));
    }
}
