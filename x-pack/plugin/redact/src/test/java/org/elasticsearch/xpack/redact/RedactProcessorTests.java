/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.redact;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.grok.GrokBuiltinPatterns;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.when;

public class RedactProcessorTests extends ESTestCase {

    private static XPackLicenseState mockLicenseState() {
        MockLicenseState licenseState = TestUtils.newMockLicenceState();
        when(licenseState.isAllowed(RedactProcessor.REDACT_PROCESSOR_FEATURE)).thenReturn(true);
        return licenseState;
    }

    private static XPackLicenseState mockNotAllowedLicenseState() {
        MockLicenseState licenseState = TestUtils.newMockLicenceState();
        when(licenseState.isAllowed(RedactProcessor.REDACT_PROCESSOR_FEATURE)).thenReturn(false);
        return licenseState;
    }

    public void testMatchRedact() throws Exception {
        {
            var config = new HashMap<String, Object>();
            config.put("field", "to_redact");
            config.put("patterns", List.of("%{EMAILADDRESS:EMAIL}"));
            var processor = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop()).create(null, "t", "d", config);
            var groks = processor.getGroks();

            {
                String input = "thisisanemail@address.com will be redacted thisisdifferent@address.com";
                var redacted = RedactProcessor.matchRedact(input, groks);
                assertEquals("<EMAIL> will be redacted <EMAIL>", redacted);
            }
            {
                String input = "This is ok nothing to redact";
                var redacted = RedactProcessor.matchRedact(input, groks);
                assertEquals(redacted, input);
            }
            {
                String input = "thisisanemail@address.com will be redacted";
                var redacted = RedactProcessor.matchRedact(input, groks);
                assertEquals("<EMAIL> will be redacted", redacted);
            }
        }
        {
            var config = new HashMap<String, Object>();
            config.put("field", "to_redact");
            config.put("patterns", List.of("%{CREDIT_CARD:CREDIT_CARD}"));
            config.put("pattern_definitions", Map.of("CREDIT_CARD", "\\b(?:\\d[ -]*?){13,16}\\b"));
            var processor = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop()).create(null, "t", "d", config);
            var groks = processor.getGroks();

            {
                String input = "here is something that looks like a credit card number: 0001-0002-0003-0004";
                var redacted = RedactProcessor.matchRedact(input, groks);
                assertEquals("here is something that looks like a credit card number: <CREDIT_CARD>", redacted);
            }
            {
                String input = "1001-1002-1003-1004 here is something that looks like a credit card number: 0001-0002-0003-0004";
                var redacted = RedactProcessor.matchRedact(input, groks);
                assertEquals("<CREDIT_CARD> here is something that looks like a credit card number: <CREDIT_CARD>", redacted);
            }
            {
                String input = "1001-1002-1003-1004 some text in between 2001-1002-1003-1004 3001-1002-1003-1004 4001-1002-1003-1004";
                var redacted = RedactProcessor.matchRedact(input, groks);
                assertEquals("<CREDIT_CARD> some text in between <CREDIT_CARD> <CREDIT_CARD> <CREDIT_CARD>", redacted);
            }
            {
                String input = "1001-1002-1003-1004 2001-1002-1003-1004 3001-1002-1003-1004 some 4001-1002-1003-1004"
                    + " and lots more text here";
                var redacted = RedactProcessor.matchRedact(input, groks);
                assertEquals("<CREDIT_CARD> <CREDIT_CARD> <CREDIT_CARD> some <CREDIT_CARD> and lots more text here", redacted);
            }
        }
        {
            var config = new HashMap<String, Object>();
            config.put("field", "to_redact");
            config.put("patterns", List.of("%{CREDIT_CARD:CREDIT_CARD}"));
            config.put("pattern_definitions", Map.of("CREDIT_CARD", "\\d{4}[ -]\\d{4}[ -]\\d{4}[ -]\\d{4}"));
            var processor = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop()).create(null, "t", "d", config);
            var grok = processor.getGroks().get(0);

            String input = "1001-1002-1003-1004 2001-1002-1003-1004 3001-1002-1003-1004 4001-1002-1003-1004";
            var redacted = RedactProcessor.matchRedact(input, List.of(grok));
            assertEquals("<CREDIT_CARD> <CREDIT_CARD> <CREDIT_CARD> <CREDIT_CARD>", redacted);
        }
    }

    public void testMatchRedactMultipleGroks() throws Exception {
        var config = new HashMap<String, Object>();
        config.put("field", "to_redact");
        config.put("patterns", List.of("%{EMAILADDRESS:EMAIL}", "%{IP:IP_ADDRESS}", "%{CREDIT_CARD:CREDIT_CARD}"));
        config.put("pattern_definitions", Map.of("CREDIT_CARD", "\\d{4}[ -]\\d{4}[ -]\\d{4}[ -]\\d{4}"));
        var processor = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop()).create(null, "t", "d", config);
        var groks = processor.getGroks();

        {
            String input = "thisisanemail@address.com will be redacted and this: 0001-0002-0003-0004 some other text";
            var redacted = RedactProcessor.matchRedact(input, groks);
            assertEquals("<EMAIL> will be redacted and this: <CREDIT_CARD> some other text", redacted);
        }
    }

    public void testRedact() throws Exception {
        var config = new HashMap<String, Object>();
        config.put("field", "to_redact");
        config.put("patterns", List.of("%{EMAILADDRESS:EMAIL}", "%{IP:IP_ADDRESS}", "%{CREDIT_CARD:CREDIT_CARD}"));
        config.put("pattern_definitions", Map.of("CREDIT_CARD", "\\d{4}[ -]\\d{4}[ -]\\d{4}[ -]\\d{4}"));
        var processor = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop()).create(null, "t", "d", config);

        {
            var ingestDoc = createIngestDoc(Map.of("to_redact", "This is ok nothing to redact"));
            var redacted = processor.execute(ingestDoc);
            assertEquals(ingestDoc, redacted);
        }
        {
            var ingestDoc = createIngestDoc(Map.of("to_redact", "thisisanemail@address.com will be redacted"));
            var redacted = processor.execute(ingestDoc);
            assertEquals("<EMAIL> will be redacted", redacted.getFieldValue("to_redact", String.class));
        }
        {
            var ingestDoc = createIngestDoc(
                Map.of("to_redact", "here is something that looks like a credit card number: 0001-0002-0003-0004")
            );
            var redacted = processor.execute(ingestDoc);
            assertEquals(
                "here is something that looks like a credit card number: <CREDIT_CARD>",
                redacted.getFieldValue("to_redact", String.class)
            );
        }
    }

    public void testRedactWithPatternNamesRedacted() throws Exception {
        var config = new HashMap<String, Object>();
        config.put("field", "to_redact");
        config.put("patterns", List.of("%{EMAILADDRESS:REDACTED}", "%{IP:REDACTED}", "%{CREDIT_CARD:REDACTED}"));
        config.put("pattern_definitions", Map.of("CREDIT_CARD", "\\d{4}[ -]\\d{4}[ -]\\d{4}[ -]\\d{4}"));
        var processor = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop()).create(null, "t", "d", config);

        {
            var ingestDoc = createIngestDoc(Map.of("to_redact", "look a credit card number! 0001-0002-0003-0004 from david@email.com"));
            var redacted = processor.execute(ingestDoc);
            assertEquals("look a credit card number! <REDACTED> from <REDACTED>", redacted.getFieldValue("to_redact", String.class));
        }
    }

    public void testDifferentStartAndEnd() throws Exception {
        {
            var config = new HashMap<String, Object>();
            config.put("field", "to_redact");
            config.put("patterns", List.of("%{EMAILADDRESS:EMAIL}", "%{IP:IP_ADDRESS}"));
            config.put("prefix", "?--");
            config.put("suffix", "}");

            var processor = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop()).create(null, "t", "d", config);
            var ingestDoc = createIngestDoc(Map.of("to_redact", "0.0.0.1 will be redacted"));
            var redacted = processor.execute(ingestDoc);
            assertEquals("?--IP_ADDRESS} will be redacted", redacted.getFieldValue("to_redact", String.class));
        }
        {
            var config = new HashMap<String, Object>();
            config.put("field", "to_redact");
            config.put("patterns", List.of("%{IP:IP_ADDRESS}"));
            config.put("prefix", "?--");

            var processor = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop()).create(null, "t", "d", config);
            var ingestDoc = createIngestDoc(Map.of("to_redact", "0.0.0.1 will be redacted"));
            var redacted = processor.execute(ingestDoc);
            assertEquals("?--IP_ADDRESS> will be redacted", redacted.getFieldValue("to_redact", String.class));
        }
        {
            var config = new HashMap<String, Object>();
            config.put("field", "to_redact");
            config.put("patterns", List.of("%{IP:IP_ADDRESS}"));
            config.put("suffix", "++");

            var processor = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop()).create(null, "t", "d", config);
            var ingestDoc = createIngestDoc(Map.of("to_redact", "0.0.0.1 will be redacted"));
            var redacted = processor.execute(ingestDoc);
            assertEquals("<IP_ADDRESS++ will be redacted", redacted.getFieldValue("to_redact", String.class));
        }
    }

    public void testIgnoreMissing() throws Exception {
        {
            var config = new HashMap<String, Object>();
            config.put("field", "to_redact");
            config.put("patterns", List.of("foo"));
            var processor = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop()).create(null, "t", "d", config);
            var ingestDoc = createIngestDoc(Map.of("not_the_field", "fieldValue"));
            var processed = processor.execute(ingestDoc);
            assertThat(ingestDoc, sameInstance(processed));
            assertEquals(ingestDoc, processed);
        }
        {
            var config = new HashMap<String, Object>();
            config.put("field", "to_redact");
            config.put("patterns", List.of("foo"));
            config.put("ignore_missing", false);   // this time the missing field should error

            var processor = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop()).create(null, "t", "d", config);
            var ingestDoc = createIngestDoc(Map.of("not_the_field", "fieldValue"));
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDoc));
            assertThat(e.getMessage(), containsString("field [to_redact] is null or missing"));
        }
    }

    public void testLicenseChecks() throws Exception {
        var notAllowed = mockNotAllowedLicenseState();
        {
            var config = new HashMap<String, Object>();
            config.put("field", "to_redact");
            config.put("patterns", List.of("foo"));
            config.put("ignore_missing", false); // usually, this would throw, but here it doesn't because of the license check
            config.put("skip_if_unlicensed", true); // set the value to true (versus using the default, which is false)
            var processor = new RedactProcessor.Factory(notAllowed, MatcherWatchdog.noop()).create(null, "t", "d", config);
            assertThat(processor.getSkipIfUnlicensed(), equalTo(true));
            var ingestDoc = createIngestDoc(Map.of("not_the_field", "fieldValue"));

            // since skip_if_unlicensed is true, the same document is returned to us unchanged
            var processed = processor.execute(ingestDoc);
            assertThat(ingestDoc, sameInstance(processed));
            assertEquals(ingestDoc, processed);
        }
        {
            // bypassing the factory, because it won't construct a processor under these circumstances
            var processor = new RedactProcessor(
                "t",
                "d",
                GrokBuiltinPatterns.ecsV1Patterns(),
                List.of("foo"),
                "to_redact",
                false, // set ignore_missing to false. usually, this would throw, but here it doesn't because of the license check
                "<",
                ">",
                MatcherWatchdog.noop(),
                notAllowed,
                false, // set skip_if_unlicensed to false, we do not want to skip, we do want to fail
                false
            );
            assertThat(processor.getSkipIfUnlicensed(), equalTo(false));
            var ingestDoc = createIngestDoc(Map.of("not_the_field", "fieldValue"));

            // since skip_if_unlicensed is false, and the license is not sufficient, we throw on execute
            ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> processor.execute(ingestDoc));
            assertThat(e.getMessage(), containsString("current license is non-compliant for [redact_processor]"));
        }
    }

    public void testLicenseChanges() throws Exception {
        // initially the license is allowed
        final boolean allowed[] = new boolean[] { true };
        MockLicenseState licenseState = TestUtils.newMockLicenceState();
        when(licenseState.isAllowed(RedactProcessor.REDACT_PROCESSOR_FEATURE)).thenAnswer(invocation -> allowed[0]);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "to_redact");
        config.put("patterns", List.of("%{MY_PATTERN:after}"));
        config.put("pattern_definitions", Map.of("MY_PATTERN", "before"));
        if (randomBoolean()) {
            config.put("skip_if_unlicensed", false); // sometimes set to false explicitly, sometimes rely on the default (also false)
        }

        // constructing the processor is allowed, including extraValidation
        RedactProcessor.Factory factory = new RedactProcessor.Factory(licenseState, MatcherWatchdog.noop());
        RedactProcessor processor = factory.create(null, null, null, config);
        processor.extraValidation();

        // it works great as long as the feature is allowed for the license
        final int times = randomIntBetween(1, 5);
        for (int i = 0; i < times; i++) {
            var ingestDoc = createIngestDoc(Map.of("to_redact", "before"));
            var redacted = processor.execute(ingestDoc);
            assertEquals("<after>", redacted.getFieldValue("to_redact", String.class));
        }

        // but stops working when the feature is not allowed for the license
        allowed[0] = false;
        for (int i = 0; i < times; i++) {
            var ingestDoc = createIngestDoc(Map.of("to_redact", "before"));
            ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> processor.execute(ingestDoc));
            assertThat(e.getMessage(), containsString("current license is non-compliant for [redact_processor]"));
        }

        // and starts working against when the license allows
        allowed[0] = true;
        for (int i = 0; i < times; i++) {
            var ingestDoc = createIngestDoc(Map.of("to_redact", "before"));
            var redacted = processor.execute(ingestDoc);
            assertEquals("<after>", redacted.getFieldValue("to_redact", String.class));
        }
    }

    @SuppressWarnings("unchecked")
    public void testTraceRedact() throws Exception {
        var config = new HashMap<String, Object>();
        config.put("field", "to_redact");
        config.put("patterns", List.of("%{EMAILADDRESS:REDACTED}"));
        config.put("trace_redact", true);
        {
            var processor = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop()).create(
                null,
                "t",
                "d",
                new HashMap<>(config)
            );
            var message = "this should not be redacted";
            var ingestDoc = createIngestDoc(Map.of("to_redact", message));
            var redactedDoc = processor.execute(ingestDoc);

            assertEquals(message, redactedDoc.getFieldValue("to_redact", String.class));
            assertNull(redactedDoc.getFieldValue(RedactProcessor.METADATA_PATH_REDACT_IS_REDACTED, Boolean.class, true));
        }
        {
            var processor = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop()).create(
                null,
                "t",
                "d",
                new HashMap<>(config)
            );
            var ingestDoc = createIngestDoc(Map.of("to_redact", "thisisanemail@address.com will be redacted"));
            var redactedDoc = processor.execute(ingestDoc);

            assertEquals("<REDACTED> will be redacted", redactedDoc.getFieldValue("to_redact", String.class));
            // validate ingest metadata path correctly resolved
            assertTrue(redactedDoc.getFieldValue(RedactProcessor.METADATA_PATH_REDACT_IS_REDACTED, Boolean.class));
            // validate ingest metadata structure correct
            var ingestMeta = redactedDoc.getIngestMetadata();
            assertTrue(ingestMeta.containsKey(RedactProcessor.REDACT_KEY));
            var redactMetadata = (HashMap<String, Object>) ingestMeta.get(RedactProcessor.REDACT_KEY);
            assertTrue(redactMetadata.containsKey(RedactProcessor.IS_REDACTED_KEY));
            assertTrue((Boolean) redactMetadata.get(RedactProcessor.IS_REDACTED_KEY));
        }
        {
            var configNoTrace = new HashMap<String, Object>();
            configNoTrace.put("field", "to_redact");
            configNoTrace.put("patterns", List.of("%{EMAILADDRESS:REDACTED}"));

            var processor = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop()).create(null, "t", "d", configNoTrace);
            var ingestDoc = createIngestDoc(Map.of("to_redact", "thisisanemail@address.com will be redacted"));
            var redactedDoc = processor.execute(ingestDoc);

            assertEquals("<REDACTED> will be redacted", redactedDoc.getFieldValue("to_redact", String.class));
            assertNull(redactedDoc.getFieldValue(RedactProcessor.METADATA_PATH_REDACT_IS_REDACTED, Boolean.class, true));
        }
    }

    public void testTraceRedactMultipleProcessors() throws Exception {
        var configRedact = new HashMap<String, Object>();
        configRedact.put("field", "to_redact");
        configRedact.put("patterns", List.of("%{EMAILADDRESS:REDACTED}"));
        configRedact.put("trace_redact", true);

        var configNoRedact = new HashMap<String, Object>();
        configNoRedact.put("field", "to_redact");
        configNoRedact.put("patterns", List.of("%{IP:REDACTED}"));  // not in the doc
        configNoRedact.put("trace_redact", true);

        // first processor does not redact doc, second one does
        {
            var processorRedact = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop()).create(
                null,
                "t1",
                "d",
                new HashMap<>(configRedact)
            );
            var processorNoRedact = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop()).create(
                null,
                "t2",
                "d",
                new HashMap<>(configNoRedact)
            );
            var ingestDocWithEmail = createIngestDoc(Map.of("to_redact", "thisisanemail@address.com will be redacted"));

            var docNotRedacted = processorNoRedact.execute(ingestDocWithEmail);
            assertNull(docNotRedacted.getFieldValue(RedactProcessor.METADATA_PATH_REDACT_IS_REDACTED, Boolean.class, true));

            var docRedacted = processorRedact.execute(docNotRedacted);
            assertTrue(docRedacted.getFieldValue(RedactProcessor.METADATA_PATH_REDACT_IS_REDACTED, Boolean.class));
        }
        // first processor redacts doc, second one does not
        {
            var processorRedact = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop()).create(
                null,
                "t1",
                "d",
                new HashMap<>(configRedact)
            );
            var processorNoRedact = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop()).create(
                null,
                "t2",
                "d",
                new HashMap<>(configNoRedact)
            );
            var ingestDocWithEmail = createIngestDoc(Map.of("to_redact", "thisisanemail@address.com will be redacted"));

            var docRedacted = processorRedact.execute(ingestDocWithEmail);
            assertTrue(docRedacted.getFieldValue(RedactProcessor.METADATA_PATH_REDACT_IS_REDACTED, Boolean.class));

            // validate does not override already redacted doc metadata
            var docRedactedAlready = processorNoRedact.execute(docRedacted);
            assertTrue(docRedactedAlready.getFieldValue(RedactProcessor.METADATA_PATH_REDACT_IS_REDACTED, Boolean.class));
        }
    }

    public void testMergeLongestRegion() {
        var r = List.of(
            new RedactProcessor.RegionTrackingMatchExtractor.Replacement(10, 20, "first"),
            new RedactProcessor.RegionTrackingMatchExtractor.Replacement(15, 28, "longest"),
            new RedactProcessor.RegionTrackingMatchExtractor.Replacement(22, 29, "third")
        );

        var merged = RedactProcessor.RegionTrackingMatchExtractor.mergeLongestRegion(r);
        assertEquals("longest", merged.patternName);
        assertEquals(10, merged.start);
        assertEquals(29, merged.end);
    }

    public void testMergeLongestRegion_smallRegionSubsumed() {
        {
            var r = List.of(
                new RedactProcessor.RegionTrackingMatchExtractor.Replacement(10, 50, "longest"),
                new RedactProcessor.RegionTrackingMatchExtractor.Replacement(15, 25, "subsumed")
            );

            var merged = RedactProcessor.RegionTrackingMatchExtractor.mergeLongestRegion(r);
            assertEquals("longest", merged.patternName);
            assertEquals(10, merged.start);
            assertEquals(50, merged.end);
        }
        {
            var r = List.of(
                new RedactProcessor.RegionTrackingMatchExtractor.Replacement(10, 50, "longest"),
                new RedactProcessor.RegionTrackingMatchExtractor.Replacement(15, 25, "subsumed"),
                new RedactProcessor.RegionTrackingMatchExtractor.Replacement(44, 60, "third")
            );

            var merged = RedactProcessor.RegionTrackingMatchExtractor.mergeLongestRegion(r);
            assertEquals("longest", merged.patternName);
            assertEquals(10, merged.start);
            assertEquals(60, merged.end);
        }
    }

    public void testMergeOverlappingReplacements_sortedByStartPositionNoOverlaps() {
        var a1 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(35, 40, "A");
        var b1 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(5, 12, "B");
        var b2 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(30, 34, "B");
        var c1 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(21, 29, "C");

        var merged = RedactProcessor.RegionTrackingMatchExtractor.mergeOverlappingReplacements(Arrays.asList(a1, b1, b2, c1));
        assertThat(merged, contains(b1, c1, b2, a1));
    }

    public void testMergeOverlappingReplacements_singleItem() {
        var l = List.of(new RedactProcessor.RegionTrackingMatchExtractor.Replacement(35, 40, "A"));
        var merged = RedactProcessor.RegionTrackingMatchExtractor.mergeOverlappingReplacements(l);
        assertThat(merged, sameInstance(l));
    }

    public void testMergeOverlappingReplacements_transitiveOverlaps() {
        {
            var a1 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(35, 40, "A");
            var b1 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(5, 10, "B");
            var b2 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(10, 15, "B");
            var c1 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(14, 29, "C");

            // b1, b2 and c1 overlap and should be merged into a single replacement
            var merged = RedactProcessor.RegionTrackingMatchExtractor.mergeOverlappingReplacements(Arrays.asList(a1, b1, b2, c1));
            assertThat(merged, hasSize(2));
            var mergedRegion = merged.get(0);
            assertEquals("C", mergedRegion.patternName);
            assertEquals(5, mergedRegion.start);
            assertEquals(29, mergedRegion.end);
            assertEquals(a1, merged.get(1));
        }
        {
            var a1 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(20, 28, "A");
            var a2 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(50, 60, "A");
            var b1 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(30, 39, "B");
            var b2 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(59, 65, "B");
            var c1 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(14, 18, "C");

            // a2 and b2 overlap
            var merged = RedactProcessor.RegionTrackingMatchExtractor.mergeOverlappingReplacements(Arrays.asList(a1, a2, b1, b2, c1));
            assertThat(merged, hasSize(4));
            assertEquals(c1, merged.get(0));
            assertEquals(a1, merged.get(1));
            assertEquals(b1, merged.get(2));
            var mergedRegion = merged.get(3);
            assertEquals("A", mergedRegion.patternName);
            assertEquals(50, mergedRegion.start);
            assertEquals(65, mergedRegion.end);
        }
        {
            var a1 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(20, 28, "A");
            var a2 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(50, 60, "A");
            var b1 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(14, 19, "B");
            var b2 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(30, 39, "B");
            var c1 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(39, 49, "C");

            var merged = RedactProcessor.RegionTrackingMatchExtractor.mergeOverlappingReplacements(Arrays.asList(a1, a2, b1, b2, c1));
            assertThat(merged, hasSize(4));
            assertEquals(b1, merged.get(0));
            assertEquals(a1, merged.get(1));
            var mergedRegion = merged.get(2);
            assertEquals("C", mergedRegion.patternName);
            assertEquals(30, mergedRegion.start);
            assertEquals(49, mergedRegion.end);
            assertEquals(a2, merged.get(3));
        }
        {
            var a1 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(20, 28, "A");
            var a2 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(50, 60, "A");
            var b1 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(22, 26, "B");

            // b1 subsumed by a1
            var merged = RedactProcessor.RegionTrackingMatchExtractor.mergeOverlappingReplacements(Arrays.asList(a1, a2, b1));
            assertThat(merged, hasSize(2));
            var mergedRegion = merged.get(0);
            assertEquals("A", mergedRegion.patternName);
            assertEquals(20, mergedRegion.start);
            assertEquals(28, mergedRegion.end);
            assertEquals(a2, merged.get(1));
        }
        {
            var a1 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(20, 28, "A");
            var a2 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(50, 60, "A");
            var b1 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(14, 21, "B");
            var b2 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(30, 36, "B");
            var c1 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(44, 51, "C");
            var c2 = new RedactProcessor.RegionTrackingMatchExtractor.Replacement(62, 70, "C");

            // a1 and b1 merged. c1 and a2 merged
            var merged = RedactProcessor.RegionTrackingMatchExtractor.mergeOverlappingReplacements(Arrays.asList(a1, a2, b1, b2, c1, c2));
            assertThat(merged, hasSize(4));
            var mergedRegion = merged.get(0);
            assertEquals("A", mergedRegion.patternName);
            assertEquals(14, mergedRegion.start);
            assertEquals(28, mergedRegion.end);
            assertEquals(b2, merged.get(1));
            mergedRegion = merged.get(2);
            assertEquals("A", mergedRegion.patternName);
            assertEquals(44, mergedRegion.start);
            assertEquals(60, mergedRegion.end);
            assertEquals(c2, merged.get(3));
        }
    }

    private IngestDocument createIngestDoc(Map<String, Object> source) {
        return new IngestDocument("index", "id", 0L, "routing", VersionType.INTERNAL, source);
    }
}
