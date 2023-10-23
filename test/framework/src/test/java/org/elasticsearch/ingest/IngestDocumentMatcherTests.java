/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;

public class IngestDocumentMatcherTests extends ESTestCase {

    public void testDifferentMapData() {
        Map<String, Object> sourceAndMetadata1 = new HashMap<>();
        sourceAndMetadata1.put("foo", "bar");
        IngestDocument document1 = TestIngestDocument.withDefaultVersion(sourceAndMetadata1);
        IngestDocument document2 = TestIngestDocument.emptyIngestDocument();
        assertThrowsOnComparision(document1, document2);
    }

    public void testDifferentLengthListData() {
        String rootKey = "foo";
        IngestDocument document1 = TestIngestDocument.withDefaultVersion(Map.of(rootKey, List.of("bar", "baz")));
        IngestDocument document2 = TestIngestDocument.withDefaultVersion(Map.of(rootKey, List.of()));
        assertThrowsOnComparision(document1, document2);
    }

    public void testDifferentNestedListFieldData() {
        String rootKey = "foo";
        IngestDocument document1 = TestIngestDocument.withDefaultVersion(Map.of(rootKey, List.of("bar", "baz")));
        IngestDocument document2 = TestIngestDocument.withDefaultVersion(Map.of(rootKey, List.of("bar", "blub")));
        assertThrowsOnComparision(document1, document2);
    }

    public void testDifferentNestedMapFieldData() {
        String rootKey = "foo";
        IngestDocument document1 = TestIngestDocument.withDefaultVersion(Map.of(rootKey, Map.of("bar", "baz")));
        IngestDocument document2 = TestIngestDocument.withDefaultVersion(Map.of(rootKey, Map.of("bar", "blub")));
        assertThrowsOnComparision(document1, document2);
    }

    public void testOnTypeConflict() {
        String rootKey = "foo";
        IngestDocument document1 = TestIngestDocument.withDefaultVersion(Map.of(rootKey, List.of("baz")));
        IngestDocument document2 = TestIngestDocument.withDefaultVersion(Map.of(rootKey, Map.of("blub", "blab")));
        assertThrowsOnComparision(document1, document2);
    }

    public void testNestedMapArrayEquivalence() {
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        // Test that equality still works when the ingest document uses primitive arrays,
        // since normal .equals() methods would not work for Maps containing these arrays.
        byte[] numbers = new byte[] { 0, 1, 2 };
        ingestDocument.setFieldValue("some.nested.array", numbers);
        IngestDocument copy = new IngestDocument(ingestDocument);
        byte[] copiedNumbers = copy.getFieldValue("some.nested.array", byte[].class);
        assertArrayEquals(numbers, copiedNumbers);
        assertNotEquals(numbers, copiedNumbers);
        assertIngestDocument(ingestDocument, copy);
    }

    public void testNullsAreEqual() {
        assertIngestDocument(null, null);
    }

    private static void assertThrowsOnComparision(IngestDocument document1, IngestDocument document2) {
        expectThrows(AssertionError.class, () -> assertIngestDocument(document1, null));
        expectThrows(AssertionError.class, () -> assertIngestDocument(null, document2));
        expectThrows(AssertionError.class, () -> assertIngestDocument(document1, document2));
        expectThrows(AssertionError.class, () -> assertIngestDocument(document2, document1));
    }
}
