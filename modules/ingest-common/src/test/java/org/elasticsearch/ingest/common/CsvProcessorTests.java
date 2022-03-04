/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.stream.Collectors;

public class CsvProcessorTests extends ESTestCase {

    private static final Character[] SEPARATORS = new Character[] { ',', ';', '|', '.', '\t' };
    private static final String[] QUOTES = new String[] { "'", "\"", "" };
    private final String quote;
    private final char separator;

    public CsvProcessorTests(@Name("quote") String quote, @Name("separator") char separator) {
        this.quote = quote;
        this.separator = separator;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        LinkedList<Object[]> list = new LinkedList<>();
        for (Character separator : SEPARATORS) {
            for (String quote : QUOTES) {
                list.add(new Object[] { quote, separator });
            }
        }
        return list;
    }

    public void testExactNumberOfFields() {
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

    public void testEmptyValues() {
        int numItems = randomIntBetween(5, 10);
        Map<String, String> items = new LinkedHashMap<>();
        for (int i = 0; i < 3; i++) {
            items.put(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10));
        }
        String emptyKey = randomAlphaOfLengthBetween(5, 10);
        items.put(emptyKey, "");
        for (int i = 0; i < numItems - 4; i++) {
            items.put(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10));
        }
        String[] headers = items.keySet().toArray(new String[numItems]);
        String csv = items.values().stream().map(v -> quote + v + quote).collect(Collectors.joining(separator + ""));

        IngestDocument ingestDocument = processDocument(headers, csv);

        items.forEach((key, value) -> {
            if (emptyKey.equals(key)) {
                assertFalse(ingestDocument.hasField(key));
            } else {
                assertEquals(value, ingestDocument.getFieldValue(key, String.class));
            }
        });
    }

    public void testEmptyValuesReplace() {
        int numItems = randomIntBetween(5, 10);
        Map<String, String> items = new LinkedHashMap<>();
        for (int i = 0; i < 3; i++) {
            items.put(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10));
        }
        String emptyKey = randomAlphaOfLengthBetween(5, 10);
        items.put(emptyKey, "");
        for (int i = 0; i < numItems - 4; i++) {
            items.put(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10));
        }
        String[] headers = items.keySet().toArray(new String[numItems]);
        String csv = items.values().stream().map(v -> quote + v + quote).collect(Collectors.joining(separator + ""));

        IngestDocument ingestDocument = processDocument(headers, csv, true, "");

        items.forEach((key, value) -> {
            if (emptyKey.equals(key)) {
                assertEquals("", ingestDocument.getFieldValue(key, String.class));
            } else {
                assertEquals(value, ingestDocument.getFieldValue(key, String.class));
            }
        });

        IngestDocument ingestDocument2 = processDocument(headers, csv, true, 0);

        items.forEach((key, value) -> {
            if (emptyKey.equals(key)) {
                assertEquals(0, (int) ingestDocument2.getFieldValue(key, Integer.class));
            } else {
                assertEquals(value, ingestDocument2.getFieldValue(key, String.class));
            }
        });
    }

    public void testLessFieldsThanHeaders() {
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

    public void testLessHeadersThanFields() {
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

    public void testSingleField() {
        String[] headers = new String[] { randomAlphaOfLengthBetween(5, 10) };
        String value = randomAlphaOfLengthBetween(5, 10);
        String csv = quote + value + quote;

        IngestDocument ingestDocument = processDocument(headers, csv);

        assertEquals(value, ingestDocument.getFieldValue(headers[0], String.class));
    }

    public void testEscapedQuote() {
        int numItems = randomIntBetween(2, 10);
        Map<String, String> items = new LinkedHashMap<>();
        for (int i = 0; i < numItems; i++) {
            items.put(
                randomAlphaOfLengthBetween(5, 10),
                randomAlphaOfLengthBetween(5, 10) + quote + quote + randomAlphaOfLengthBetween(5, 10) + quote + quote
            );
        }
        String[] headers = items.keySet().toArray(new String[numItems]);
        String csv = items.values().stream().map(v -> quote + v + quote).collect(Collectors.joining(separator + ""));

        IngestDocument ingestDocument = processDocument(headers, csv);

        items.forEach((key, value) -> assertEquals(value.replace(quote + quote, quote), ingestDocument.getFieldValue(key, String.class)));
    }

    public void testQuotedStrings() {
        assumeFalse("quote needed", quote.isEmpty());
        int numItems = randomIntBetween(2, 10);
        Map<String, String> items = new LinkedHashMap<>();
        for (int i = 0; i < numItems; i++) {
            items.put(
                randomAlphaOfLengthBetween(5, 10),
                separator + randomAlphaOfLengthBetween(5, 10) + separator + "\n\r" + randomAlphaOfLengthBetween(5, 10)
            );
        }
        String[] headers = items.keySet().toArray(new String[numItems]);
        String csv = items.values().stream().map(v -> quote + v + quote).collect(Collectors.joining(separator + ""));

        IngestDocument ingestDocument = processDocument(headers, csv);

        items.forEach((key, value) -> assertEquals(value.replace(quote + quote, quote), ingestDocument.getFieldValue(key, String.class)));
    }

    public void testEmptyFields() {
        int numItems = randomIntBetween(5, 10);
        Map<String, String> items = new LinkedHashMap<>();
        for (int i = 0; i < numItems; i++) {
            items.put(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10));
        }
        String[] headers = items.keySet().toArray(new String[numItems]);
        String csv = items.values()
            .stream()
            .map(v -> quote + v + quote)
            .limit(numItems - 1)
            .skip(3)
            .collect(Collectors.joining(separator + ""));

        IngestDocument ingestDocument = processDocument(
            headers,
            "" + separator + "" + separator + "" + separator + csv + separator + separator + "abc"
        );

        items.keySet().stream().limit(3).forEach(key -> assertFalse(ingestDocument.hasField(key)));
        items.entrySet()
            .stream()
            .limit(numItems - 1)
            .skip(3)
            .forEach(e -> assertEquals(e.getValue(), ingestDocument.getFieldValue(e.getKey(), String.class)));
        items.keySet().stream().skip(numItems - 1).forEach(key -> assertFalse(ingestDocument.hasField(key)));
    }

    public void testWrongStrings() throws Exception {
        assumeTrue("single run only", quote.isEmpty());
        expectThrows(IllegalArgumentException.class, () -> processDocument(new String[] { "a" }, "abc\"abc"));
        expectThrows(IllegalArgumentException.class, () -> processDocument(new String[] { "a" }, "\"abc\"asd"));
        expectThrows(IllegalArgumentException.class, () -> processDocument(new String[] { "a" }, "\"abcasd"));
        expectThrows(IllegalArgumentException.class, () -> processDocument(new String[] { "a" }, "abc\nabc"));
        expectThrows(IllegalArgumentException.class, () -> processDocument(new String[] { "a" }, "abc\rabc"));
    }

    public void testQuotedWhitespaces() {
        assumeFalse("quote needed", quote.isEmpty());
        IngestDocument document = processDocument(
            new String[] { "a", "b", "c", "d" },
            "  abc   " + separator + " def" + separator + "ghi  " + separator + " " + quote + "  ooo  " + quote
        );
        assertEquals("abc", document.getFieldValue("a", String.class));
        assertEquals("def", document.getFieldValue("b", String.class));
        assertEquals("ghi", document.getFieldValue("c", String.class));
        assertEquals("  ooo  ", document.getFieldValue("d", String.class));
    }

    public void testUntrimmed() {
        assumeFalse("quote needed", quote.isEmpty());
        IngestDocument document = processDocument(
            new String[] { "a", "b", "c", "d", "e", "f" },
            "  abc   "
                + separator
                + " def"
                + separator
                + "ghi  "
                + separator
                + "   "
                + quote
                + "ooo"
                + quote
                + "    "
                + separator
                + "  "
                + quote
                + "jjj"
                + quote
                + "   ",
            false
        );
        assertEquals("  abc   ", document.getFieldValue("a", String.class));
        assertEquals(" def", document.getFieldValue("b", String.class));
        assertEquals("ghi  ", document.getFieldValue("c", String.class));
        assertEquals("ooo", document.getFieldValue("d", String.class));
        assertEquals("jjj", document.getFieldValue("e", String.class));
        assertFalse(document.hasField("f"));
    }

    public void testIgnoreMissing() {
        assumeTrue("single run only", quote.isEmpty());
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = randomAlphaOfLength(5);
        if (ingestDocument.hasField(fieldName)) {
            ingestDocument.removeField(fieldName);
        }
        CsvProcessor processor = new CsvProcessor(
            randomAlphaOfLength(5),
            null,
            fieldName,
            new String[] { "a" },
            false,
            ',',
            '"',
            true,
            null
        );
        processor.execute(ingestDocument);
        CsvProcessor processor2 = new CsvProcessor(
            randomAlphaOfLength(5),
            null,
            fieldName,
            new String[] { "a" },
            false,
            ',',
            '"',
            false,
            null
        );
        expectThrows(IllegalArgumentException.class, () -> processor2.execute(ingestDocument));
    }

    public void testEmptyHeaders() throws Exception {
        assumeTrue("single run only", quote.isEmpty());
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "abc,abc");
        HashMap<String, Object> metadata = new HashMap<>(ingestDocument.getSourceAndMetadata());

        CsvProcessor processor = new CsvProcessor(randomAlphaOfLength(5), null, fieldName, new String[0], false, ',', '"', false, null);

        processor.execute(ingestDocument);

        assertEquals(metadata, ingestDocument.getSourceAndMetadata());
    }

    private IngestDocument processDocument(String[] headers, String csv) {
        return processDocument(headers, csv, true);
    }

    private IngestDocument processDocument(String[] headers, String csv, boolean trim) {
        return processDocument(headers, csv, trim, null);
    }

    private IngestDocument processDocument(String[] headers, String csv, boolean trim, Object emptyValue) {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Arrays.stream(headers).filter(ingestDocument::hasField).forEach(ingestDocument::removeField);
        String fieldName = randomAlphaOfLength(11);
        ingestDocument.setFieldValue(fieldName, csv);

        char quoteChar = quote.isEmpty() ? '"' : quote.charAt(0);
        CsvProcessor processor = new CsvProcessor(
            randomAlphaOfLength(5),
            null,
            fieldName,
            headers,
            trim,
            separator,
            quoteChar,
            false,
            emptyValue
        );

        processor.execute(ingestDocument);

        return ingestDocument;
    }
}
