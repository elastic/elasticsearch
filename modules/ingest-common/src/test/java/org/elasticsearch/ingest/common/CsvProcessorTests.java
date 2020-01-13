/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest.common;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class CsvProcessorTests extends ESTestCase {

    private static final Character[] SEPARATORS = new Character[]{',', ';', '|', '.'};
    private final String quote;
    private char separator;


    public CsvProcessorTests(@Name("quote") String quote) {
        this.quote = quote;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[]{"'"}, new Object[]{"\""}, new Object[]{""});
    }

    @Before
    public void setup() {
        separator = randomFrom(SEPARATORS);
    }

    public void testExactNumberOfFields() throws Exception {
        int numItems = randomIntBetween(2, 10);
        Map<String, String> items = new LinkedHashMap<>();
        for (int i = 0; i < numItems; i++) {
            items.put(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10));
        }
        String[] headers = items.keySet().toArray(new String[numItems]);
        String csv = items.values().stream().map(v -> quote + v + quote).collect(Collectors.joining(separator + ""));

        IngestDocument ingestDocument = processDocument(headers, csv);

        items.forEach((key, value) -> assertEquals(value, ingestDocument.getFieldValue(key, String.class)));
    }

    public void testLessFieldsThanHeaders() throws Exception {
        int numItems = randomIntBetween(4, 10);
        Map<String, String> items = new LinkedHashMap<>();
        for (int i = 0; i < numItems; i++) {
            items.put(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10));
        }
        String[] headers = items.keySet().toArray(new String[numItems]);
        String csv = items.values().stream().map(v -> quote + v + quote).limit(3).collect(Collectors.joining(separator + ""));

        IngestDocument ingestDocument = processDocument(headers, csv);

        items.keySet().stream().skip(3).forEach(key -> assertFalse(ingestDocument.hasField(key)));
        items.entrySet().stream().limit(3).forEach(e -> assertEquals(e.getValue(), ingestDocument.getFieldValue(e.getKey(), String.class)));
    }

    public void testLessHeadersThanFields() throws Exception {
        int numItems = randomIntBetween(5, 10);
        Map<String, String> items = new LinkedHashMap<>();
        for (int i = 0; i < numItems; i++) {
            items.put(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10));
        }
        String[] headers = items.keySet().stream().limit(3).toArray(String[]::new);
        String csv = items.values().stream().map(v -> quote + v + quote).collect(Collectors.joining(separator + ""));

        IngestDocument ingestDocument = processDocument(headers, csv);

        items.entrySet().stream().limit(3).forEach(e -> assertEquals(e.getValue(), ingestDocument.getFieldValue(e.getKey(), String.class)));
    }

    public void testSingleField() throws Exception {
        String[] headers = new String[]{randomAlphaOfLengthBetween(5, 10)};
        String value = randomAlphaOfLengthBetween(5, 10);
        String csv = quote + value + quote;

        IngestDocument ingestDocument = processDocument(headers, csv);

        assertEquals(value, ingestDocument.getFieldValue(headers[0], String.class));
    }

    public void testEscapedQuote() throws Exception {
        int numItems = randomIntBetween(2, 10);
        Map<String, String> items = new LinkedHashMap<>();
        for (int i = 0; i < numItems; i++) {
            items.put(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10) + quote + quote + randomAlphaOfLengthBetween(5
                , 10) + quote + quote);
        }
        String[] headers = items.keySet().toArray(new String[numItems]);
        String csv = items.values().stream().map(v -> quote + v + quote).collect(Collectors.joining(separator + ""));

        IngestDocument ingestDocument = processDocument(headers, csv);

        items.forEach((key, value) -> assertEquals(value.replace(quote + quote, quote), ingestDocument.getFieldValue(key, String.class)));
    }

    public void testQuotedStrings() throws Exception {
        assumeFalse("quote needed", quote.isEmpty());
        int numItems = randomIntBetween(2, 10);
        Map<String, String> items = new LinkedHashMap<>();
        for (int i = 0; i < numItems; i++) {
            items.put(randomAlphaOfLengthBetween(5, 10),
                separator + randomAlphaOfLengthBetween(5, 10) + separator + "\n\r" + randomAlphaOfLengthBetween(5, 10));
        }
        String[] headers = items.keySet().toArray(new String[numItems]);
        String csv = items.values().stream().map(v -> quote + v + quote).collect(Collectors.joining(separator + ""));

        IngestDocument ingestDocument = processDocument(headers, csv);

        items.forEach((key, value) -> assertEquals(value.replace(quote + quote, quote), ingestDocument.getFieldValue(key,
            String.class)));
    }

    public void testEmptyFields() throws Exception {
        int numItems = randomIntBetween(5, 10);
        Map<String, String> items = new LinkedHashMap<>();
        for (int i = 0; i < numItems; i++) {
            items.put(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10));
        }
        String[] headers = items.keySet().toArray(new String[numItems]);
        String csv =
            items.values().stream().map(v -> quote + v + quote).limit(numItems - 1).skip(3).collect(Collectors.joining(separator + ""));

        IngestDocument ingestDocument = processDocument(headers,
            "" + separator + "" + separator + "" + separator + csv + separator + separator +
                "abc");

        items.keySet().stream().limit(3).forEach(key -> assertFalse(ingestDocument.hasField(key)));
        items.entrySet().stream().limit(numItems - 1).skip(3).forEach(e -> assertEquals(e.getValue(),
            ingestDocument.getFieldValue(e.getKey(), String.class)));
        items.keySet().stream().skip(numItems - 1).forEach(key -> assertFalse(ingestDocument.hasField(key)));
    }

    public void testWrongStings() throws Exception {
        assumeTrue("single run only", quote.isEmpty());
        expectThrows(IllegalArgumentException.class, () -> processDocument(new String[]{"a"}, "abc\"abc"));
        expectThrows(IllegalArgumentException.class, () -> processDocument(new String[]{"a"}, "\"abc\"asd"));
        expectThrows(IllegalArgumentException.class, () -> processDocument(new String[]{"a"}, "\"abcasd"));
        expectThrows(IllegalArgumentException.class, () -> processDocument(new String[]{"a"}, "abc\nabc"));
        expectThrows(IllegalArgumentException.class, () -> processDocument(new String[]{"a"}, "abc\rabc"));
    }

    public void testQuotedWhitespaces() throws Exception {
        assumeFalse("quote needed", quote.isEmpty());
        IngestDocument document = processDocument(new String[]{"a", "b", "c", "d"},
            "  abc   " + separator + " def" + separator + "ghi  " + separator + " " + quote + "  ooo  " + quote);
        assertEquals("abc", document.getFieldValue("a", String.class));
        assertEquals("def", document.getFieldValue("b", String.class));
        assertEquals("ghi", document.getFieldValue("c", String.class));
        assertEquals("  ooo  ", document.getFieldValue("d", String.class));
    }

    public void testUntrimmed() throws Exception {
        assumeFalse("quote needed", quote.isEmpty());
        IngestDocument document = processDocument(new String[]{"a", "b", "c", "d", "e", "f"},
            "  abc   " + separator + " def" + separator + "ghi  " + separator + "   "
                + quote + "ooo" + quote + "    " + separator + "  " + quote + "jjj" + quote + "   ", false);
        assertEquals("  abc   ", document.getFieldValue("a", String.class));
        assertEquals(" def", document.getFieldValue("b", String.class));
        assertEquals("ghi  ", document.getFieldValue("c", String.class));
        assertEquals("ooo", document.getFieldValue("d", String.class));
        assertEquals("jjj", document.getFieldValue("e", String.class));
        assertFalse(document.hasField("f"));
    }

    public void testEmptyHeaders() throws Exception {
        assumeTrue("single run only", quote.isEmpty());
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "abc,abc");
        HashMap<String, Object> metadata = new HashMap<>(ingestDocument.getSourceAndMetadata());

        CsvProcessor processor = new CsvProcessor(randomAlphaOfLength(5), fieldName, new String[0], false, ',', '"', false);

        processor.execute(ingestDocument);

        assertEquals(metadata, ingestDocument.getSourceAndMetadata());
    }

    private IngestDocument processDocument(String[] headers, String csv) throws Exception {
        return processDocument(headers, csv, true);
    }

    private IngestDocument processDocument(String[] headers, String csv, boolean trim) throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Arrays.stream(headers).filter(ingestDocument::hasField).forEach(ingestDocument::removeField);

        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, csv);
        char quoteChar = quote.isEmpty() ? '"' : quote.charAt(0);
        CsvProcessor processor = new CsvProcessor(randomAlphaOfLength(5), fieldName, headers, trim, separator, quoteChar, false);

        processor.execute(ingestDocument);

        return ingestDocument;
    }
}
