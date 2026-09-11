/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.AbstractColumnarMapperCompatibilityTestCase;
import org.elasticsearch.indices.recovery.RecoverySettings;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class FlattenedFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "flat";

    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    public void testSingleKey() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").endObject()),
            columnarSettings(),
            batch("single key", 1L, doc("d1", 1L, "{\"flat\":{\"key1\":\"value1\"}}"))
        );
    }

    public void testMultipleKeys() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").endObject()),
            columnarSettings(),
            batch("multiple keys", 1L, doc("d1", 1L, "{\"flat\":{\"key1\":\"a\",\"key2\":\"b\",\"key3\":\"c\"}}"))
        );
    }

    /** A nested object collapses to a dotted key, exactly as the row path's {@code ContentPath} does. */
    public void testNestedObject() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").endObject()),
            columnarSettings(),
            batch("nested object", 1L, doc("d1", 1L, "{\"flat\":{\"outer\":{\"inner\":\"v\"},\"top\":\"t\"}}"))
        );
    }

    /** A literal dotted key produces the same leaf as the equivalent nested object. */
    public void testLiteralDottedKey() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").endObject()),
            columnarSettings(),
            batch("literal dotted key", 1L, doc("d1", 1L, "{\"flat\":{\"a.b\":\"v\",\"a.b.c\":\"w\"}}"))
        );
    }

    /** Array order and duplicates must survive: the keyed channel is document-order with no dedup. */
    public void testArrayValuesPreserveOrderAndDuplicates() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").endObject()),
            columnarSettings(),
            batch(
                "array order and duplicates",
                1L,
                doc("d1", 1L, "{\"flat\":{\"k\":[\"c\",\"a\",\"b\",\"a\"]}}"),
                doc("d2", 2L, "{\"flat\":{\"k\":[\"z\"]}}"),
                doc("d3", 3L, "{\"flat\":{\"k\":[\"m\",\"m\",\"m\"]}}")
            )
        );
    }

    public void testMultipleKeysWithArrays() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").endObject()),
            columnarSettings(),
            batch(
                "multiple keys with arrays",
                1L,
                doc("d1", 1L, "{\"flat\":{\"k1\":[\"a\",\"b\"],\"k2\":\"single\",\"k3\":[\"x\",\"y\",\"z\"]}}")
            )
        );
    }

    /** A JSON null with no {@code null_value} becomes a null slot carrying its key inline. */
    public void testExplicitNull() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").endObject()),
            columnarSettings(),
            batch(
                "explicit null",
                1L,
                doc("d1", 1L, "{\"flat\":{\"k\":null}}"),
                doc("d2", 2L, "{\"flat\":{\"k\":\"present\"}}"),
                doc("d3", 3L, "{\"flat\":{\"k\":[\"a\",null,\"b\"]}}")
            )
        );
    }

    /** An all-null document still writes a keyed blob, because null slots carry their keys. */
    public void testAllNullDocument() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").endObject()),
            columnarSettings(),
            batch("all null document", 1L, doc("d1", 1L, "{\"flat\":{\"k1\":null,\"k2\":null}}"))
        );
    }

    public void testNullValueSubstitution() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").field("null_value", "NULL").endObject()),
            columnarSettings(),
            batch(
                "null_value substitution",
                1L,
                doc("d1", 1L, "{\"flat\":{\"k\":null}}"),
                doc("d2", 2L, "{\"flat\":{\"k\":\"real\"}}"),
                doc("d3", 3L, "{\"flat\":{\"k\":[null,\"x\"]}}")
            )
        );
    }

    /**
     * An empty-string value and a null share the same slot bytes ({@code key\0}) and differ only in the length prefix — {@code 1}
     * versus {@code 0}. This is the one case where getting the null marker wrong is invisible in the payload.
     */
    public void testEmptyStringDistinctFromNull() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").endObject()),
            columnarSettings(),
            batch(
                "empty string vs null",
                1L,
                doc("d1", 1L, "{\"flat\":{\"k\":\"\"}}"),
                doc("d2", 2L, "{\"flat\":{\"k\":null}}"),
                doc("d3", 3L, "{\"flat\":{\"k\":[\"\",null,\"\"]}}")
            )
        );
    }

    /** An absent field, an explicit null at the field path, and an empty object all emit nothing. */
    public void testAbsentNullAndEmptyObject() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").endObject()),
            columnarSettings(),
            batch(
                "absent, null and empty object",
                1L,
                doc("d1", 1L, "{}"),
                doc("d2", 2L, "{\"flat\":null}"),
                doc("d3", 3L, "{\"flat\":{}}"),
                doc("d4", 4L, "{\"flat\":{\"k\":\"v\"}}")
            )
        );
    }

    /**
     * Non-string scalars go through the same canonical stringification keyword uses. Only canonical literals are used here: the
     * columnar path renders parsed values, so {@code 1.50} would become {@code 1.5} while the row path preserves the source text.
     */
    public void testScalarValues() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").endObject()),
            columnarSettings(),
            batch(
                "scalar values",
                1L,
                doc("d1", 1L, "{\"flat\":{\"n\":42,\"d\":1.5,\"t\":true,\"f\":false}}"),
                doc("d2", 2L, "{\"flat\":{\"n\":-7,\"d\":0.25,\"t\":false,\"f\":true}}")
            )
        );
    }

    public void testScalarArrays() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").endObject()),
            columnarSettings(),
            batch("scalar arrays", 1L, doc("d1", 1L, "{\"flat\":{\"n\":[1,2,3],\"b\":[true,false]}}"))
        );
    }

    /** Sparse batch: docs without the field leave the output columns sparse, exercising the tuples cursor branch. */
    public void testSparseBatch() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").endObject()),
            columnarSettings(),
            batch(
                "sparse batch",
                1L,
                doc("d1", 1L, "{\"flat\":{\"k\":\"a\"}}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"flat\":{\"k\":\"b\"}}"),
                doc("d4", 4L, "{}"),
                doc("d5", 5L, "{\"flat\":{\"k\":\"c\"}}")
            )
        );
    }

    /** Keys present in only some documents: each column is independently sparse across the batch. */
    public void testKeysPresentInSomeDocsOnly() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").endObject()),
            columnarSettings(),
            batch(
                "keys present in some docs only",
                1L,
                doc("d1", 1L, "{\"flat\":{\"k1\":\"a\"}}"),
                doc("d2", 2L, "{\"flat\":{\"k1\":\"b\",\"k2\":\"c\"}}"),
                doc("d3", 3L, "{\"flat\":{\"k2\":\"d\"}}"),
                doc("d4", 4L, "{\"flat\":{\"k1\":\"e\",\"k2\":\"f\",\"k3\":\"g\"}}")
            )
        );
    }

    public void testNonAsciiKeysAndValues() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").endObject()),
            columnarSettings(),
            batch(
                "non-ascii",
                1L,
                doc("d1", 1L, "{\"flat\":{\"ключ\":\"значение\"}}"),
                doc("d2", 2L, "{\"flat\":{\"键\":\"值\",\"café\":\"au lait\"}}")
            )
        );
    }

    public void testMultiDocSharedKeys() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").endObject()),
            columnarSettings(),
            batch(
                "multi-doc shared keys",
                2L,
                doc("d1", 1L, "{\"flat\":{\"host\":\"a\",\"port\":8080}}"),
                doc("d2", 2L, "{\"flat\":{\"host\":\"b\",\"port\":443}}"),
                doc("d3", 3L, "{\"flat\":{\"host\":\"c\",\"port\":22}}")
            )
        );
    }

    public void testFullMix() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").endObject()),
            columnarSettings(),
            batch(
                "full mix",
                3L,
                doc("d1", 1L, "{\"flat\":{\"a\":\"x\",\"b\":[1,2],\"c\":null}}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"flat\":{\"a\":\"\",\"b\":[],\"c\":true}}"),
                doc("d4", 4L, "{\"flat\":null}"),
                doc("d5", 5L, "{\"flat\":{\"a\":\"ünïcøde\",\"nested\":{\"deep\":\"v\"}}}"),
                doc("d6", 6L, "{\"flat\":{}}")
            )
        );
    }

    /**
     * Values over {@code ignore_above} belong in the {@code _keyed._ignored} channel, which the columnar path does not yet write, so
     * it bails to make the production driver fall back to the row path.
     */
    public void testIgnoreAboveIsRejected() {
        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> assertColumnarMatchesXContent(
                mapping(b -> b.startObject(FIELD).field("type", "flattened").field("ignore_above", 4).endObject()),
                columnarSettings(),
                batch("ignore_above exceeded", 1L, doc("d1", 1L, "{\"flat\":{\"k\":\"too long\"}}"))
            )
        );
        assertThat(e.getMessage(), containsString("exceeds ignore_above"));
    }

    /** A value at or below {@code ignore_above} is mapped normally. */
    public void testIgnoreAboveNotExceeded() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").field("ignore_above", 8).endObject()),
            columnarSettings(),
            batch(
                "ignore_above not exceeded",
                1L,
                doc("d1", 1L, "{\"flat\":{\"k\":\"exactly8\"}}"),
                doc("d2", 2L, "{\"flat\":{\"k\":\"short\"}}")
            )
        );
    }

    /** {@code \0} is the reserved key/value separator, so a key containing it is rejected on both paths. */
    public void testKeyWithReservedSeparatorIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> assertColumnarMatchesXContent(
                mapping(b -> b.startObject(FIELD).field("type", "flattened").endObject()),
                columnarSettings(),
                batch("reserved separator in key", 1L, doc("d1", 1L, "{\"flat\":{\"bad\\u0000key\":\"v\"}}"))
            )
        );
        assertThat(e.getMessage(), containsString("cannot contain the reserved character"));
    }

    /**
     * A relative key whose nesting depth exceeds {@code depth_limit} throws {@link IllegalArgumentException}
     * on the columnar path, mirroring the row path's {@code FlattenedFieldParser.validateDepthLimit}.
     *
     * <p>The mapping uses {@code depth_limit: 2}, so a key like {@code a.b.c} (dot-count = 2,
     * effective depth = 3) should be rejected while {@code a.b} (dot-count = 1, effective depth = 2)
     * is accepted.
     */
    public void testDepthLimitExceededThrowsIllegalArgument() {
        // "a.b.c" has dot-count 2, effective depth 3 > depth_limit 2 → rejected.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> assertColumnarMatchesXContent(
                mapping(b -> b.startObject(FIELD).field("type", "flattened").field("depth_limit", 2).endObject()),
                columnarSettings(),
                batch("depth_limit exceeded", 1L, doc("d1", 1L, "{\"flat\":{\"a\":{\"b\":{\"c\":\"deep\"}}}}"))
            )
        );
        assertThat(e.getMessage(), containsString("exceeds the maximum depth limit"));
        assertThat(e.getMessage(), containsString("[2]"));
    }

    /**
     * A key exactly at {@code depth_limit} (dot-count = depth_limit - 1) must pass on both paths.
     * Guards against an off-by-one in the check.
     */
    public void testDepthLimitAtBoundaryPasses() throws IOException {
        // depth_limit = 2 → keys with dot-count ≤ 1 (effective depth ≤ 2) are accepted.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").field("depth_limit", 2).endObject()),
            columnarSettings(),
            batch(
                "depth at boundary",
                1L,
                doc("d1", 1L, "{\"flat\":{\"a\":{\"b\":\"ok\"}}}"), // dot-count 1, depth 2 = limit → accepted
                doc("d2", 2L, "{\"flat\":{\"top\":\"v\"}}") // dot-count 0, depth 1 < limit → accepted
            )
        );
    }

    /**
     * A literal dotted key inside the flattened value ({@code {"flat":{"a.b":"v"}}}) produces the same
     * field set as the equivalent nested object ({@code {"flat":{"a":{"b":"v"}}}}), on both paths.
     */
    public void testLiteralDottedKeyMatchesNestedObject() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").endObject()),
            columnarSettings(),
            batch(
                "literal dotted key vs nested object",
                1L,
                doc("d1", 1L, "{\"flat\":{\"a.b\":\"v\"}}"),
                doc("d2", 2L, "{\"flat\":{\"a\":{\"b\":\"v\"}}}")
            )
        );
    }

    /**
     * Both spellings of the same key inside a <em>single</em> document produce two group columns with the identical
     * relative key {@code a.b}, both present on that document. This is the case that breaks a per-leaf mapper — it
     * would emit two independent outputs — but a group mapper receives every column in one
     * {@link FlattenedFieldMapper#mapColumnGroupBatch} call and merges both slots into the same per-document blob.
     */
    public void testBothSpellingsOfOneKeyInASingleDocument() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "flattened").endObject()),
            columnarSettings(),
            batch(
                "both spellings in one doc",
                1L,
                doc("d1", 1L, "{\"flat\":{\"a.b\":\"one\",\"a\":{\"b\":\"two\"}}}"),
                doc("d2", 2L, "{\"flat\":{\"a.b\":\"only\"}}")
            )
        );
    }
}
