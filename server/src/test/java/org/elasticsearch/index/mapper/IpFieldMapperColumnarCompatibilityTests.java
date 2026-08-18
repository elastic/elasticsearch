/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.recovery.RecoverySettings;

import java.io.IOException;

public class IpFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "f";

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
}
