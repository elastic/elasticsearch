/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.record;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Rendering actual results in csv-spec table syntax, and the per-query row caps. */
public class CsvSpecRecorderTests extends ESTestCase {

    public void testRenderTable() {
        String table = CsvSpecRecorder.renderTable(
            List.of(Map.of("name", "c", "type", "long"), Map.of("name", "AdvEngineID", "type", "integer")),
            List.of(List.of(1669L, 2), List.of(563L, 13), Arrays.asList(null, 62))
        );
        assertEquals("""
            c:long | AdvEngineID:integer
            1669 | 2
            563 | 13
            null | 62
            """, table);
    }

    public void testRenderMultiValue() {
        assertEquals("[a, b]", CsvSpecRecorder.renderValue(List.of("a", "b")));
        assertEquals("null", CsvSpecRecorder.renderValue(null));
        assertEquals("42", CsvSpecRecorder.renderValue(42));
    }

    public void testRecordWritesFragment() throws IOException {
        Path dir = createTempDir();
        new CsvSpecRecorder(dir).record("public-clickbench.csv-spec", new RecordedFragment("q01", "label-x", "c:long\n42\n"));
        Path file = dir.resolve("public-clickbench.csv-spec.q01.label-x.recorded");
        assertTrue(Files.exists(file));
        String content = Files.readString(file);
        assertTrue(content.contains("NEVER a source of expected values"));
        assertTrue(content.contains("c:long"));
    }

    public void testResultLimits() {
        ResultLimits.enforce("q", 300, 300);
        AssertionError overDeclared = expectThrows(AssertionError.class, () -> ResultLimits.enforce("q", 300, 301));
        assertTrue(overDeclared.getMessage().contains("above its declared max-rows"));
        AssertionError overAbsolute = expectThrows(AssertionError.class, () -> ResultLimits.enforce("q", 1001, 5));
        assertTrue(overAbsolute.getMessage().contains("absolute cap"));
    }
}
