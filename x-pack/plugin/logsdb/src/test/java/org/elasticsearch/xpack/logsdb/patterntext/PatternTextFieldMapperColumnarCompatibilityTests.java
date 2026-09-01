/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.AbstractColumnarMapperCompatibilityTestCase;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.logsdb.LogsDBPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class PatternTextFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "f";

    @Override
    protected Collection<Plugin> getPlugins() {
        return List.of(new LogsDBPlugin(Settings.EMPTY));
    }

    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    public void testSingleValueWithArgs() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "pattern_text").endObject()),
            columnarSettings(),
            batch("single value with args", 1L, doc("d1", 1L, "{\"f\":\"Error 123 at line 456\"}"))
        );
    }

    public void testSingleValueNoArgs() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "pattern_text").endObject()),
            columnarSettings(),
            batch("single value no args", 1L, doc("d1", 1L, "{\"f\":\"No numbers here\"}"))
        );
    }

    public void testSingleValueAllArgs() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "pattern_text").endObject()),
            columnarSettings(),
            batch("all args", 1L, doc("d1", 1L, "{\"f\":\"123 456 789\"}"))
        );
    }

    public void testMultiDocBatch() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "pattern_text").endObject()),
            columnarSettings(),
            batch(
                "multi-doc batch",
                1L,
                doc("d1", 1L, "{\"f\":\"Error 123 occurred\"}"),
                doc("d2", 2L, "{\"f\":\"Warning 456 at line 789\"}"),
                doc("d3", 3L, "{\"f\":\"No numbers here\"}")
            )
        );
    }

    public void testDocsWithSharedTemplate() throws IOException {
        // Multiple docs sharing the same template — common for log data sorted by template_id.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "pattern_text").endObject()),
            columnarSettings(),
            batch(
                "shared template",
                1L,
                doc("d1", 1L, "{\"f\":\"Connection refused: 127.0.0.1:8080\"}"),
                doc("d2", 2L, "{\"f\":\"Connection refused: 10.0.0.1:443\"}"),
                doc("d3", 3L, "{\"f\":\"Connection refused: 192.168.1.1:22\"}")
            )
        );
    }

    public void testFieldAbsentInSomeDocs() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "pattern_text").endObject()),
            columnarSettings(),
            batch(
                "sparse docs",
                1L,
                doc("d1", 1L, "{\"f\":\"Error 123\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":\"Warning 456\"}"),
                doc("d4", 4L, "{}"),
                doc("d5", 5L, "{\"f\":\"Info 789\"}")
            )
        );
    }

    public void testExplicitNull() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "pattern_text").endObject()),
            columnarSettings(),
            batch("explicit null", 1L, doc("d1", 1L, "{\"f\":null}"), doc("d2", 2L, "{\"f\":\"Value 42\"}"), doc("d3", 3L, "{\"f\":null}"))
        );
    }

    public void testLeadingTrailingDelimiters() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "pattern_text").endObject()),
            columnarSettings(),
            batch(
                "leading trailing delimiters",
                1L,
                doc("d1", 1L, "{\"f\":\"  leading 123\"}"),
                doc("d2", 2L, "{\"f\":\"trailing 456  \"}"),
                doc("d3", 3L, "{\"f\":\"[bracket 789]\"}")
            )
        );
    }

    public void testConsecutiveDelimiters() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "pattern_text").endObject()),
            columnarSettings(),
            batch("consecutive delimiters", 1L, doc("d1", 1L, "{\"f\":\"a  b  c 42\"}"), doc("d2", 2L, "{\"f\":\"x\\ty\\n99\"}"))
        );
    }

    public void testNonAsciiValues() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "pattern_text").endObject()),
            columnarSettings(),
            batch(
                "non-ascii",
                1L,
                doc("d1", 1L, "{\"f\":\"Ошибка 123 в строке 456\"}"),  // Cyrillic with digits
                doc("d2", 2L, "{\"f\":\"错误 789\"}"),                    // CJK with digits
                doc("d3", 3L, "{\"f\":\"café au lait\"}")               // Latin extended, no digits
            )
        );
    }

    public void testIndexOptionsPositions() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "pattern_text").field("index_options", "positions").endObject()),
            columnarSettings(),
            batch(
                "positions",
                1L,
                doc("d1", 1L, "{\"f\":\"Error 123 at line 456\"}"),
                doc("d2", 2L, "{\"f\":\"Warning: no numbers here\"}")
            )
        );
    }

    public void testDisableTemplating() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "pattern_text").field("disable_templating", true).endObject()),
            columnarSettings(),
            batch(
                "disable templating",
                1L,
                doc("d1", 1L, "{\"f\":\"Error 123 occurred\"}"),
                doc("d2", 2L, "{\"f\":\"No numbers here\"}"),
                doc("d3", 3L, "{\"f\":null}"),
                doc("d4", 4L, "{}")
            )
        );
    }

    public void testStandardAnalyzer() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "pattern_text").field("analyzer", "standard").endObject()),
            columnarSettings(),
            batch(
                "standard analyzer",
                1L,
                doc("d1", 1L, "{\"f\":\"Error 123 occurred\"}"),
                doc("d2", 2L, "{\"f\":\"Warning at line 456\"}")
            )
        );
    }

    /**
     * Verifies the LENGTH_EXCEEDED path for values over 8 192 UTF-16 chars. These docs must emit
     * {@code template_id} and raw text ({@code .stored}), but no template/args columns.
     * A batch that mixes normal and over-limit docs exercises the sparse column layout.
     */
    public void testMixedNormalAndLengthExceeded() throws IOException {
        // Build a value that exceeds the 8192-char limit.
        String longValue = "token ".repeat(1500); // ~9000 chars, no args, prefix "token " is non-arg
        // Also build a longer value with digits to test the args path for exceeded values.
        String longValueWithDigit = "x 42 " + "token ".repeat(1500);

        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "pattern_text").endObject()),
            columnarSettings(),
            batch(
                "mixed normal and length exceeded",
                1L,
                doc("d1", 1L, "{\"f\":\"Error 123 at line 456\"}"),            // TEMPLATED
                doc("d2", 2L, "{\"f\":\"" + longValue.trim() + "\"}"),          // LENGTH_EXCEEDED
                doc("d3", 3L, "{\"f\":\"Warning 789\"}"),                       // TEMPLATED
                doc("d4", 4L, "{\"f\":\"" + longValueWithDigit.trim() + "\"}"), // LENGTH_EXCEEDED
                doc("d5", 5L, "{\"f\":\"Info 0 at startup\"}")                  // TEMPLATED
            )
        );
    }

    public void testAllLengthExceeded() throws IOException {
        String longValue = "word ".repeat(2000); // ~10000 chars
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "pattern_text").endObject()),
            columnarSettings(),
            batch(
                "all length exceeded",
                1L,
                doc("d1", 1L, "{\"f\":\"" + longValue.trim() + "\"}"),
                doc("d2", 2L, "{\"f\":\"" + longValue.trim() + "  extra\"}")
            )
        );
    }

    public void testManyArgs() throws IOException {
        // A log line with many numeric tokens exercises the arg-offset buffer growth.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "pattern_text").endObject()),
            columnarSettings(),
            batch("many args", 1L, doc("d1", 1L, "{\"f\":\"a 1 b 2 c 3 d 4 e 5 f 6 g 7 h 8 i 9 j 10\"}"))
        );
    }

    public void testFullMix() throws IOException {
        // A batch that exercises many code paths at once.
        String longValue = "word ".repeat(2000);
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "pattern_text").endObject()),
            columnarSettings(),
            batch(
                "full mix",
                2L, // non-trivial primary term
                doc("d1", 1L, "{\"f\":\"Error 123 at line 456\"}"),
                doc("d2", 2L, "{\"f\":\"No numbers here\"}"),
                doc("d3", 3L, "{}"),
                doc("d4", 4L, "{\"f\":null}"),
                doc("d5", 5L, "{\"f\":\"  leading 789  \"}"),
                doc("d6", 6L, "{\"f\":\"" + longValue.trim() + "\"}"),
                doc("d7", 7L, "{\"f\":\"Ошибка 42\"}")
            )
        );
    }
}
