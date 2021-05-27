/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.equalTo;


public class GrokProcessorTests extends ESTestCase {

    public void testMatch() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "1");
        GrokProcessor processor = new GrokProcessor(randomAlphaOfLength(10), null, Collections.singletonMap("ONE", "1"),
            Collections.singletonList("%{ONE:one}"), fieldName, false, false, MatcherWatchdog.noop());
        processor.execute(doc);
        assertThat(doc.getFieldValue("one", String.class), equalTo("1"));
    }

    public void testIgnoreCase() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "A");
        GrokProcessor processor = new GrokProcessor(randomAlphaOfLength(10), null, Collections.emptyMap(),
            Collections.singletonList("(?<a>(?i)A)"), fieldName, false, false, MatcherWatchdog.noop());
        processor.execute(doc);
        assertThat(doc.getFieldValue("a", String.class), equalTo("A"));
    }

    public void testNoMatch() {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "23");
        GrokProcessor processor = new GrokProcessor(randomAlphaOfLength(10), null, Collections.singletonMap("ONE", "1"),
            Collections.singletonList("%{ONE:one}"), fieldName, false, false, MatcherWatchdog.noop());
        Exception e = expectThrows(Exception.class, () -> processor.execute(doc));
        assertThat(e.getMessage(), equalTo("Provided Grok expressions do not match field value: [23]"));
    }

    public void testNoMatchingPatternName() {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "23");
        Exception e = expectThrows(IllegalArgumentException.class, () -> new GrokProcessor(randomAlphaOfLength(10),
                null, Collections.singletonMap("ONE", "1"), Collections.singletonList("%{NOTONE:not_one}"), fieldName,
            false, false, MatcherWatchdog.noop()));
        assertThat(e.getMessage(), equalTo("Unable to find pattern [NOTONE] in Grok's pattern dictionary"));
    }

    public void testMatchWithoutCaptures() throws Exception {
        String fieldName = "value";
        IngestDocument originalDoc = new IngestDocument(new HashMap<>(), new HashMap<>());
        originalDoc.setFieldValue(fieldName, fieldName);
        IngestDocument doc = new IngestDocument(originalDoc);
        GrokProcessor processor = new GrokProcessor(randomAlphaOfLength(10), null, Collections.emptyMap(),
            Collections.singletonList(fieldName), fieldName, false, false, MatcherWatchdog.noop());
        processor.execute(doc);
        assertThat(doc, equalTo(originalDoc));
    }

    public void testNullField() {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, null);
        GrokProcessor processor = new GrokProcessor(randomAlphaOfLength(10), null, Collections.singletonMap("ONE", "1"),
            Collections.singletonList("%{ONE:one}"), fieldName, false, false, MatcherWatchdog.noop());
        Exception e = expectThrows(Exception.class, () -> processor.execute(doc));
        assertThat(e.getMessage(), equalTo("field [" + fieldName + "] is null, cannot process it."));
    }

    public void testNullFieldWithIgnoreMissing() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        originalIngestDocument.setFieldValue(fieldName, null);
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        GrokProcessor processor = new GrokProcessor(randomAlphaOfLength(10), null, Collections.singletonMap("ONE", "1"),
            Collections.singletonList("%{ONE:one}"), fieldName, false, true, MatcherWatchdog.noop());
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNotStringField() {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, 1);
        GrokProcessor processor = new GrokProcessor(randomAlphaOfLength(10), null, Collections.singletonMap("ONE", "1"),
            Collections.singletonList("%{ONE:one}"), fieldName, false, false, MatcherWatchdog.noop());
        Exception e = expectThrows(Exception.class, () -> processor.execute(doc));
        assertThat(e.getMessage(), equalTo("field [" + fieldName + "] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));
    }

    public void testNotStringFieldWithIgnoreMissing() {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, 1);
        GrokProcessor processor = new GrokProcessor(randomAlphaOfLength(10), null, Collections.singletonMap("ONE", "1"),
            Collections.singletonList("%{ONE:one}"), fieldName, false, true, MatcherWatchdog.noop());
        Exception e = expectThrows(Exception.class, () -> processor.execute(doc));
        assertThat(e.getMessage(), equalTo("field [" + fieldName + "] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));
    }

    public void testMissingField() {
        String fieldName = "foo.bar";
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        GrokProcessor processor = new GrokProcessor(randomAlphaOfLength(10), null, Collections.singletonMap("ONE", "1"),
            Collections.singletonList("%{ONE:one}"), fieldName, false, false, MatcherWatchdog.noop());
        Exception e = expectThrows(Exception.class, () -> processor.execute(doc));
        assertThat(e.getMessage(), equalTo("field [foo] not present as part of path [foo.bar]"));
    }

    public void testMissingFieldWithIgnoreMissing() throws Exception {
        String fieldName = "foo.bar";
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        GrokProcessor processor = new GrokProcessor(randomAlphaOfLength(10), null, Collections.singletonMap("ONE", "1"),
            Collections.singletonList("%{ONE:one}"), fieldName, false, true, MatcherWatchdog.noop());
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testMultiplePatternsWithMatchReturn() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "2");
        Map<String, String> patternBank = new HashMap<>();
        patternBank.put("ONE", "1");
        patternBank.put("TWO", "2");
        patternBank.put("THREE", "3");
        GrokProcessor processor = new GrokProcessor(randomAlphaOfLength(10), null, patternBank,
            Arrays.asList("%{ONE:one}", "%{TWO:two}", "%{THREE:three}"), fieldName, false, false, MatcherWatchdog.noop());
        processor.execute(doc);
        assertThat(doc.hasField("one"), equalTo(false));
        assertThat(doc.getFieldValue("two", String.class), equalTo("2"));
        assertThat(doc.hasField("three"), equalTo(false));
    }

    public void testSetMetadata() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "abc23");
        Map<String, String> patternBank = new HashMap<>();
        patternBank.put("ONE", "1");
        patternBank.put("TWO", "2");
        patternBank.put("THREE", "3");
        GrokProcessor processor = new GrokProcessor(randomAlphaOfLength(10), null, patternBank,
            Arrays.asList("%{ONE:one}", "%{TWO:two}", "%{THREE:three}"), fieldName, true, false, MatcherWatchdog.noop());
        processor.execute(doc);
        assertThat(doc.hasField("one"), equalTo(false));
        assertThat(doc.getFieldValue("two", String.class), equalTo("2"));
        assertThat(doc.hasField("three"), equalTo(false));
        assertThat(doc.getFieldValue("_ingest._grok_match_index", String.class), equalTo("1"));
    }

    public void testTraceWithOnePattern() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "first1");
        Map<String, String> patternBank = new HashMap<>();
        patternBank.put("ONE", "1");
        GrokProcessor processor = new GrokProcessor(randomAlphaOfLength(10), null, patternBank,
            Arrays.asList("%{ONE:one}"), fieldName, true, false, MatcherWatchdog.noop());
        processor.execute(doc);
        assertThat(doc.hasField("one"), equalTo(true));
        assertThat(doc.getFieldValue("_ingest._grok_match_index", String.class), equalTo("0"));
    }

    public void testCombinedPatterns() {
        String combined;
        combined = GrokProcessor.combinePatterns(Arrays.asList(""), false);
        assertThat(combined, equalTo(""));
        combined = GrokProcessor.combinePatterns(Arrays.asList(""), true);
        assertThat(combined, equalTo(""));
        combined = GrokProcessor.combinePatterns(Arrays.asList("foo"), false);
        assertThat(combined, equalTo("foo"));
        combined = GrokProcessor.combinePatterns(Arrays.asList("foo"), true);
        assertThat(combined, equalTo("foo"));
        combined = GrokProcessor.combinePatterns(Arrays.asList("foo", "bar"), false);
        assertThat(combined, equalTo("(?:foo)|(?:bar)"));
        combined = GrokProcessor.combinePatterns(Arrays.asList("foo", "bar"), true);
        assertThat(combined, equalTo("(?<_ingest._grok_match_index.0>foo)|(?<_ingest._grok_match_index.1>bar)"));
    }

    public void testCombineSamePatternNameAcrossPatterns() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "1-3");
        Map<String, String> patternBank = new HashMap<>();
        patternBank.put("ONE", "1");
        patternBank.put("TWO", "2");
        patternBank.put("THREE", "3");
        GrokProcessor processor = new GrokProcessor(randomAlphaOfLength(10), null, patternBank, Arrays.asList("%{ONE:first}-%{TWO:second}",
            "%{ONE:first}-%{THREE:second}"), fieldName, randomBoolean(), randomBoolean(), MatcherWatchdog.noop());
        processor.execute(doc);
        assertThat(doc.getFieldValue("first", String.class), equalTo("1"));
        assertThat(doc.getFieldValue("second", String.class), equalTo("3"));
    }

    public void testFirstWinNamedCapture() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "12");
        Map<String, String> patternBank = new HashMap<>();
        patternBank.put("ONETWO", "1|2");
        GrokProcessor processor = new GrokProcessor(randomAlphaOfLength(10), null, patternBank,
            Collections.singletonList("%{ONETWO:first}%{ONETWO:first}"), fieldName, randomBoolean(), randomBoolean(),
            MatcherWatchdog.noop());
        processor.execute(doc);
        assertThat(doc.getFieldValue("first", String.class), equalTo("1"));
    }

    public void testUnmatchedNamesNotIncludedInDocument()  throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "3");
        Map<String, String> patternBank = new HashMap<>();
        patternBank.put("ONETWO", "1|2");
        patternBank.put("THREE", "3");
        GrokProcessor processor = new GrokProcessor(randomAlphaOfLength(10), null, patternBank,
            Collections.singletonList("%{ONETWO:first}|%{THREE:second}"), fieldName, randomBoolean(), randomBoolean(),
            MatcherWatchdog.noop());
        processor.execute(doc);
        assertFalse(doc.hasField("first"));
        assertThat(doc.getFieldValue("second", String.class), equalTo("3"));
    }
}
