/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.grok.GrokCaptureExtracter;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.sameInstance;

public class RedactProcessorTests extends ESTestCase {

    public void testRedactGroks() throws Exception {
        var config = new HashMap<String, Object>();
        config.put("field", "to_redact");
        config.put("patterns", List.of("%{EMAILADDRESS:EMAIL}", "%{IP:IP_ADDRESS}", "%{CREDIT_CARD:CREDIT_CARD}"));
        config.put("pattern_definitions", Map.of("CREDIT_CARD", "\\d{4}[ -]\\d{4}[ -]\\d{4}[ -]\\d{4}"));
        var processor = new RedactProcessor.Factory(MatcherWatchdog.noop()).create(null, "t", "d", config);
        var groks = processor.getGroks();

        {
            String input = "This is ok nothing to redact";
            var redacted = RedactProcessor.redactGroks(input, groks);
            assertThat(redacted, sameInstance(input));
        }
        {
            String input = "thisisanemail@address.com will be redacted";
            var redacted = RedactProcessor.redactGroks(input, groks);
            assertEquals("<EMAIL> will be redacted", redacted);
        }
        {
            String input = "here is something that looks like a credit card number: 0001-0002-0003-0004";
            var redacted = RedactProcessor.redactGroks(input, groks);
            assertEquals("here is something that looks like a credit card number: <CREDIT_CARD>", redacted);
        }
    }

    public void testInlineRedactGroks() throws Exception {
        var config = new HashMap<String, Object>();
        config.put("field", "to_redact");
        config.put("patterns", List.of("%{EMAILADDRESS:EMAIL}"));
//        config.put("patterns", List.of("%{CREDIT_CARD:CREDIT_CARD}"));
//        config.put("patterns", List.of("%{EMAILADDRESS:EMAIL}", "%{IP:IP_ADDRESS}", "%{CREDIT_CARD:CREDIT_CARD}"));
        config.put("pattern_definitions", Map.of("CREDIT_CARD", "\\d{4}[-]\\d{4}[-]\\d{4}[-]\\d{4}"));
        var processor = new RedactProcessor.Factory(MatcherWatchdog.noop()).create(null, "t", "d", config);
        var groks = processor.getGroks();

//        {
//            String input = "thisisanemail@address.com will be redacted thisisdifferent@address.com";
//            var redacted = RedactProcessor.inplaceRedact(input, groks);
//            assertEquals("<EMAIL> will be redacted <EMAIL>", redacted);
//        }
//        {
//            String input = "This is ok nothing to redact";
//            var redacted = RedactProcessor.inplaceRedact(input, groks);
//            assertEquals(redacted, input);
//        }
//        {
//            String input = "thisisanemail@address.com will be redacted";
//            var redacted = RedactProcessor.inplaceRedact(input, groks);
//            assertEquals("<EMAIL> will be redacted", redacted);
//        }
//        {
//            String input = "here is something that looks like a credit card number: 0001-0002-0003-0004";
//            var redacted = RedactProcessor.inplaceRedact(input, groks);
//            assertEquals("here is something that looks like a credit card number: <CREDIT_CARD>", redacted);
//        }
//        {
//            String input = "1001-1002-1003-1004 here is something that looks like a credit card number: 0001-0002-0003-0004 ";
//            var redacted = RedactProcessor.inplaceRedact(input, groks);
//            assertEquals("<CREDIT_CARD> here is something that looks like a credit card number: <CREDIT_CARD> ", redacted);
//        }
        {
            var config2 = new HashMap<String, Object>();
            config2.put("field", "to_redact");
        config2.put("patterns", List.of("%{CREDIT_CARD:CREDIT_CARD}"));
            config2.put("pattern_definitions", Map.of("CREDIT_CARD", "\\d{4}[-]\\d{4}[-]\\d{4}[-]\\d{4}"));
            var processor2 = new RedactProcessor.Factory(MatcherWatchdog.noop()).create(null, "t", "d", config2);
            var groks2 = processor2.getGroks();

            String input = "1001-1002-1003-1004 2001-1002-1003-1004 3001-1002-1003-1004 4001-1002-1003-1004";
            var redacted = RedactProcessor.inplaceRedact(input, groks2);
            assertEquals("<CREDIT_CARD> <CREDIT_CARD> <CREDIT_CARD>", redacted);
        }
        // TODO overlapping regions
    }

    public void testExtractAll() throws Exception {
        var config2 = new HashMap<String, Object>();
        config2.put("field", "to_redact");
        config2.put("patterns", List.of("%{CREDIT_CARD:CREDIT_CARD}"));
        config2.put("pattern_definitions", Map.of("CREDIT_CARD", "\\d{4}[-]\\d{4}[-]\\d{4}[-]\\d{4}"));
        var processor2 = new RedactProcessor.Factory(MatcherWatchdog.noop()).create(null, "t", "d", config2);
        var grok = processor2.getGroks().get(0);

        String input = "1001-1002-1003-1004 2001-1002-1003-1004 3001-1002-1003-1004 4001-1002-1003-1004";
        var redacted = RedactProcessor.extractAll(input, grok);
        assertEquals("<CREDIT_CARD> <CREDIT_CARD> <CREDIT_CARD>", redacted);
    }



    public void testRedactGroksMultipleMatches() throws Exception {
        var config = new HashMap<String, Object>();
        config.put("field", "to_redact");
        config.put("patterns", List.of("%{EMAILADDRESS:EMAIL}", "%{IP:IP_ADDRESS}", "%{CREDIT_CARD:CREDIT_CARD}"));
        config.put("pattern_definitions", Map.of("CREDIT_CARD", "\\d{4}[ -]\\d{4}[ -]\\d{4}[ -]\\d{4}"));
        var processor = new RedactProcessor.Factory(MatcherWatchdog.noop()).create(null, "t", "d", config);
        var groks = processor.getGroks();
        {
            String input = "thisisanemail@address.com will be redacted thisisdifferent@address.com";
            var redacted = RedactProcessor.redactGroks(input, groks);
            assertEquals("<EMAIL> will be redacted <email>", redacted);
        }
        {
            String input = "1001-1002-1003-1004 here is something that looks like a credit card number: 0001-0002-0003-0004";
            var redacted = RedactProcessor.redactGroks(input, groks);
            assertEquals("<CREDIT_CARD> here is something that looks like a credit card number: <CREDIT_CARD>", redacted);
        }
    }

    public void testRedact() throws Exception {
        var config = new HashMap<String, Object>();
        config.put("field", "to_redact");
        config.put("patterns", List.of("%{EMAILADDRESS:EMAIL}", "%{IP:IP_ADDRESS}", "%{CREDIT_CARD:CREDIT_CARD}"));
        config.put("pattern_definitions", Map.of("CREDIT_CARD", "\\d{4}[ -]\\d{4}[ -]\\d{4}[ -]\\d{4}"));
        var processor = new RedactProcessor.Factory(MatcherWatchdog.noop()).create(null, "t", "d", config);
        var groks = processor.getGroks();

        {
            var ingestDoc = createIngestDoc(Map.of("to_redact", "This is ok nothing to redact"));
            var redacted = processor.execute(ingestDoc);
            assertThat(redacted, sameInstance(ingestDoc));
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

    public void testIgnoreMissing() throws Exception {
        {
            var config = new HashMap<String, Object>();
            config.put("field", "to_redact");
            config.put("patterns", List.of("foo"));
            var processor = new RedactProcessor.Factory(MatcherWatchdog.noop()).create(null, "t", "d", config);
            var ingestDoc = createIngestDoc(Map.of("not_the_field", "fieldValue"));
            var processed = processor.execute(ingestDoc);
            assertThat(ingestDoc, sameInstance(processed));
        }
        {
            var config = new HashMap<String, Object>();
            config.put("field", "to_redact");
            config.put("patterns", List.of("foo"));
            config.put("ignore_missing", false);   // this time the missing field should error

            var processor = new RedactProcessor.Factory(MatcherWatchdog.noop()).create(null, "t", "d", config);
            var ingestDoc = createIngestDoc(Map.of("not_the_field", "fieldValue"));
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDoc));
            assertThat(e.getMessage(), containsString("field [to_redact] is null or missing"));
        }
    }

    private IngestDocument createIngestDoc(Map<String, Object> source) {
        return new IngestDocument("index", "id", 0L, "routing", VersionType.INTERNAL, source);
    }
}
