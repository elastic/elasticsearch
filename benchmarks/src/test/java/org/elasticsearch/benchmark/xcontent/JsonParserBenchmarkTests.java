/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.xcontent;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

public class JsonParserBenchmarkTests extends ESTestCase {

    public void testClasspathSourceProducesSingleParseable() throws IOException {
        var benchmark = new JsonParserBenchmark();
        benchmark.source = "monitor_cluster_stats.json";
        benchmark.mode = "split";
        benchmark.setup();

        assertEquals(1, benchmark.docs.length);
        assertParseable(benchmark.docs[0]);
    }

    public void testFilesystemNdjsonSplitModeProducesOneDocPerLine() throws IOException {
        var tmp = createTempFile("bench", ".ndjson");
        Files.write(tmp, List.of("{\"a\":1}", "{\"b\":\"hello\"}", "{\"c\":true}"));

        var benchmark = new JsonParserBenchmark();
        benchmark.source = tmp.toString();
        benchmark.mode = "split";
        benchmark.setup();

        assertEquals(3, benchmark.docs.length);
        for (byte[] doc : benchmark.docs) {
            assertParseable(doc);
        }
    }

    public void testFilesystemBlankLinesSkipped() throws IOException {
        var tmp = createTempFile("bench", ".ndjson");
        Files.write(tmp, List.of("{\"a\":1}", "", "{\"b\":2}", ""));

        var benchmark = new JsonParserBenchmark();
        benchmark.source = tmp.toString();
        benchmark.mode = "split";
        benchmark.setup();

        assertEquals(2, benchmark.docs.length);
    }

    public void testFilesystemStreamModeProducesSingleElement() throws IOException {
        var tmp = createTempFile("bench", ".ndjson");
        Files.write(tmp, List.of("{\"a\":1}", "{\"b\":\"hello\"}", "{\"c\":true}"));

        var benchmark = new JsonParserBenchmark();
        benchmark.source = tmp.toString();
        benchmark.mode = "stream";
        benchmark.setup();

        assertEquals(1, benchmark.docs.length);
    }

    public void testParseToMapThrowsForStreamMode() throws IOException {
        var tmp = createTempFile("bench", ".ndjson");
        Files.write(tmp, List.of("{\"a\":1}"));

        var benchmark = new JsonParserBenchmark();
        benchmark.source = tmp.toString();
        benchmark.mode = "stream";
        benchmark.setup();

        assertThrows(UnsupportedOperationException.class, () -> benchmark.parseToMap(null, null));
    }

    public void testInvalidSourceThrows() {
        var benchmark = new JsonParserBenchmark();
        benchmark.source = "no-such-file.json";
        benchmark.mode = "split";
        assertThrows(IllegalArgumentException.class, benchmark::setup);
    }

    private static void assertParseable(byte[] doc) throws IOException {
        try (var parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, doc, 0, doc.length)) {
            Map<String, Object> map = parser.mapOrdered();
            assertNotNull(map);
            assertFalse(map.isEmpty());
        }
    }
}
