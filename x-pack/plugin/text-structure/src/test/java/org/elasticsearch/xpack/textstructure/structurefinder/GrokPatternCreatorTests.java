/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.textstructure.structurefinder.GrokPatternCreator.ValueOnlyGrokPatternCandidate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class GrokPatternCreatorTests extends TextStructureTestCase {

    private static final boolean ECS_COMPATIBILITY_V1 = true;
    private static final boolean ECS_COMPATIBILITY_DISABLED = false;

    public void testBuildFieldName() {
        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        assertEquals("field", GrokPatternCreator.buildFieldName(fieldNameCountStore, "field"));
        assertEquals("field2", GrokPatternCreator.buildFieldName(fieldNameCountStore, "field"));
        assertEquals("field3", GrokPatternCreator.buildFieldName(fieldNameCountStore, "field"));
        assertEquals("extra_timestamp", GrokPatternCreator.buildFieldName(fieldNameCountStore, "extra_timestamp"));
        assertEquals("field4", GrokPatternCreator.buildFieldName(fieldNameCountStore, "field"));
        assertEquals("uri", GrokPatternCreator.buildFieldName(fieldNameCountStore, "uri"));
        assertEquals("extra_timestamp2", GrokPatternCreator.buildFieldName(fieldNameCountStore, "extra_timestamp"));
        assertEquals("field5", GrokPatternCreator.buildFieldName(fieldNameCountStore, "field"));
    }

    public void testPopulatePrefacesAndEpiloguesGivenTimestamp() {

        Collection<String> matchingStrings = Arrays.asList(
            "[2018-01-25T15:33:23] DEBUG ",
            "[2018-01-24T12:33:23] ERROR ",
            "junk [2018-01-22T07:33:23] INFO ",
            "[2018-01-21T03:33:23] DEBUG "
        );

        for (boolean ECS_COMPATIBILITY_MODE : Arrays.asList(ECS_COMPATIBILITY_DISABLED, ECS_COMPATIBILITY_V1)) {
            ValueOnlyGrokPatternCandidate candidate = new ValueOnlyGrokPatternCandidate(
                "TIMESTAMP_ISO8601",
                "date",
                "extra_timestamp",
                ECS_COMPATIBILITY_MODE
            );

            Map<String, Integer> fieldNameCountStore = new HashMap<>();
            Collection<String> prefaces = new ArrayList<>();
            Collection<String> epilogues = new ArrayList<>();
            candidate.processCaptures(
                explanation,
                fieldNameCountStore,
                matchingStrings,
                prefaces,
                epilogues,
                null,
                null,
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_MODE
            );

            assertThat(prefaces, containsInAnyOrder("[", "[", "junk [", "["));
            assertThat(epilogues, containsInAnyOrder("] DEBUG ", "] ERROR ", "] INFO ", "] DEBUG "));
        }
    }

    public void testPopulatePrefacesAndEpiloguesGivenEmailAddress() {

        Collection<String> matchingStrings = Arrays.asList("before alice@acme.com after", "abc bob@acme.com xyz", "carol@acme.com");

        for (boolean ECS_COMPATIBILITY_MODE : Arrays.asList(ECS_COMPATIBILITY_DISABLED, ECS_COMPATIBILITY_V1)) {
            ValueOnlyGrokPatternCandidate candidate = new ValueOnlyGrokPatternCandidate(
                "EMAILADDRESS",
                "keyword",
                "email",
                ECS_COMPATIBILITY_MODE
            );

            Map<String, Integer> fieldNameCountStore = new HashMap<>();
            Collection<String> prefaces = new ArrayList<>();
            Collection<String> epilogues = new ArrayList<>();

            candidate.processCaptures(
                explanation,
                fieldNameCountStore,
                matchingStrings,
                prefaces,
                epilogues,
                null,
                null,
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_MODE
            );

            assertThat(prefaces, containsInAnyOrder("before ", "abc ", ""));
            assertThat(epilogues, containsInAnyOrder(" after", " xyz", ""));
        }
    }

    public void testAppendBestGrokMatchForStringsGivenTimestampsAndLogLevels() {

        Collection<String> snippets = Arrays.asList(
            "[2018-01-25T15:33:23] DEBUG ",
            "[2018-01-24T12:33:23] ERROR ",
            "junk [2018-01-22T07:33:23] INFO ",
            "[2018-01-21T03:33:23] DEBUG "
        );

        GrokPatternCreator[] grokPatternCreators = new GrokPatternCreator[] {
            new GrokPatternCreator(
                explanation,
                snippets,
                null,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            ),
            new GrokPatternCreator(explanation, snippets, null, null, Collections.emptyMap(), NOOP_TIMEOUT_CHECKER, ECS_COMPATIBILITY_V1) };

        String[] expectedLoglevel = new String[] { "loglevel", "log.level" };

        int index = 0;
        for (GrokPatternCreator grokPatternCreator : grokPatternCreators) {
            grokPatternCreator.appendBestGrokMatchForStrings(false, snippets, false, 0);

            String loglevel = expectedLoglevel[index];
            assertEquals(
                ".*?\\[%{TIMESTAMP_ISO8601:extra_timestamp}\\] %{LOGLEVEL:" + loglevel + "} ",
                grokPatternCreator.getOverallGrokPatternBuilder().toString()
            );

            ++index;
        }
    }

    public void testAppendBestGrokMatchForStringsGivenNumbersInBrackets() {

        Collection<String> snippets = Arrays.asList("(-2)", "  (-3)", " (4)", " (-5) ");

        GrokPatternCreator[] grokPatternCreators = new GrokPatternCreator[] {
            new GrokPatternCreator(
                explanation,
                snippets,
                null,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            ),
            new GrokPatternCreator(explanation, snippets, null, null, Collections.emptyMap(), NOOP_TIMEOUT_CHECKER, ECS_COMPATIBILITY_V1) };

        for (GrokPatternCreator grokPatternCreator : grokPatternCreators) {
            grokPatternCreator.appendBestGrokMatchForStrings(false, snippets, false, 0);

            assertEquals(".*?\\(%{INT:field}\\).*?", grokPatternCreator.getOverallGrokPatternBuilder().toString());
        }
    }

    public void testAppendBestGrokMatchForStringsGivenNegativeNumbersWithoutBreak() {

        Collection<String> snippets = Arrays.asList("before-2 ", "prior to-3", "-4");

        GrokPatternCreator[] grokPatternCreators = new GrokPatternCreator[] {
            new GrokPatternCreator(
                explanation,
                snippets,
                null,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            ),
            new GrokPatternCreator(explanation, snippets, null, null, Collections.emptyMap(), NOOP_TIMEOUT_CHECKER, ECS_COMPATIBILITY_V1) };

        for (GrokPatternCreator grokPatternCreator : grokPatternCreators) {
            grokPatternCreator.appendBestGrokMatchForStrings(false, snippets, false, 0);

            // It seems sensible that we don't detect these suffices as either base 10 or base 16 numbers
            assertEquals(".*?", grokPatternCreator.getOverallGrokPatternBuilder().toString());
        }
    }

    public void testAppendBestGrokMatchForStringsGivenHexNumbers() {

        Collection<String> snippets = Arrays.asList(" abc", "  123", " -123", "1f is hex");

        GrokPatternCreator[] grokPatternCreators = new GrokPatternCreator[] {
            new GrokPatternCreator(
                explanation,
                snippets,
                null,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            ),
            new GrokPatternCreator(explanation, snippets, null, null, Collections.emptyMap(), NOOP_TIMEOUT_CHECKER, ECS_COMPATIBILITY_V1) };

        for (GrokPatternCreator grokPatternCreator : grokPatternCreators) {
            grokPatternCreator.appendBestGrokMatchForStrings(false, snippets, false, 0);

            assertEquals(".*?%{BASE16NUM:field}.*?", grokPatternCreator.getOverallGrokPatternBuilder().toString());
        }
    }

    public void testAppendBestGrokMatchForStringsGivenHostnamesWithNumbers() {

        Collection<String> snippets = Arrays.asList("<host1.1.p2ps:", "<host2.1.p2ps:");

        GrokPatternCreator[] grokPatternCreators = new GrokPatternCreator[] {
            new GrokPatternCreator(
                explanation,
                snippets,
                null,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            ),
            new GrokPatternCreator(explanation, snippets, null, null, Collections.emptyMap(), NOOP_TIMEOUT_CHECKER, ECS_COMPATIBILITY_V1) };

        for (GrokPatternCreator grokPatternCreator : grokPatternCreators) {
            grokPatternCreator.appendBestGrokMatchForStrings(false, snippets, false, 0);

            // We don't want the .1. in the middle to get detected as a hex number
            assertEquals("<.*?:", grokPatternCreator.getOverallGrokPatternBuilder().toString());
        }
    }

    public void testAppendBestGrokMatchForStringsGivenEmailAddresses() {

        Collection<String> snippets = Arrays.asList("before alice@acme.com after", "abc bob@acme.com xyz", "carol@acme.com");

        GrokPatternCreator[] grokPatternCreators = new GrokPatternCreator[] {
            new GrokPatternCreator(
                explanation,
                snippets,
                null,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            ),
            new GrokPatternCreator(explanation, snippets, null, null, Collections.emptyMap(), NOOP_TIMEOUT_CHECKER, ECS_COMPATIBILITY_V1) };

        for (GrokPatternCreator grokPatternCreator : grokPatternCreators) {
            grokPatternCreator.appendBestGrokMatchForStrings(false, snippets, false, 0);

            assertEquals(".*?%{EMAILADDRESS:email}.*?", grokPatternCreator.getOverallGrokPatternBuilder().toString());
        }
    }

    public void testAppendBestGrokMatchForStringsGivenUris() {

        Collection<String> snippets = Arrays.asList(
            "main site https://www.elastic.co/ with trailing slash",
            "https://www.elastic.co/guide/en/x-pack/current/ml-configuring-categories.html#ml-configuring-categories is a section",
            "download today from https://www.elastic.co/downloads"
        );

        GrokPatternCreator[] grokPatternCreators = new GrokPatternCreator[] {
            new GrokPatternCreator(
                explanation,
                snippets,
                null,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            ),
            new GrokPatternCreator(explanation, snippets, null, null, Collections.emptyMap(), NOOP_TIMEOUT_CHECKER, ECS_COMPATIBILITY_V1) };

        String[] expectedUris = new String[] { "uri", "url.original" };

        int index = 0;
        for (GrokPatternCreator grokPatternCreator : grokPatternCreators) {
            String expectedUri = expectedUris[index];
            grokPatternCreator.appendBestGrokMatchForStrings(false, snippets, false, 0);

            assertEquals(".*?%{URI:" + expectedUri + "}.*?", grokPatternCreator.getOverallGrokPatternBuilder().toString());

            ++index;
        }
    }

    public void testAppendBestGrokMatchForStringsGivenPaths() {

        Collection<String> snippets = Arrays.asList("on Mac /Users/dave", "on Windows C:\\Users\\dave", "on Linux /home/dave");

        GrokPatternCreator[] grokPatternCreators = new GrokPatternCreator[] {
            new GrokPatternCreator(
                explanation,
                snippets,
                null,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            ),
            new GrokPatternCreator(explanation, snippets, null, null, Collections.emptyMap(), NOOP_TIMEOUT_CHECKER, ECS_COMPATIBILITY_V1) };

        for (GrokPatternCreator grokPatternCreator : grokPatternCreators) {
            grokPatternCreator.appendBestGrokMatchForStrings(false, snippets, false, 0);

            assertEquals(".*? .*? %{PATH:path}", grokPatternCreator.getOverallGrokPatternBuilder().toString());
        }
    }

    public void testAppendBestGrokMatchForStringsGivenKvPairs() {

        Collection<String> snippets = Arrays.asList(
            "foo=1 and bar=a",
            "something foo=2 bar=b something else",
            "foo=3 bar=c",
            " foo=1 bar=a "
        );

        GrokPatternCreator[] grokPatternCreators = new GrokPatternCreator[] {
            new GrokPatternCreator(
                explanation,
                snippets,
                null,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            ),
            new GrokPatternCreator(explanation, snippets, null, null, Collections.emptyMap(), NOOP_TIMEOUT_CHECKER, ECS_COMPATIBILITY_V1) };

        for (GrokPatternCreator grokPatternCreator : grokPatternCreators) {
            grokPatternCreator.appendBestGrokMatchForStrings(false, snippets, false, 0);

            assertEquals(".*?\\bfoo=%{USER:foo} .*?\\bbar=%{USER:bar}.*?", grokPatternCreator.getOverallGrokPatternBuilder().toString());
        }
    }

    public void testCreateGrokPatternFromExamplesGivenNamedLogs() {

        Collection<String> sampleMessages = Arrays.asList(
            "Sep  8 11:55:06 linux named[22529]: error (unexpected RCODE REFUSED) resolving 'elastic.slack.com/A/IN': 95.110.64.205#53",
            "Sep  8 11:55:08 linux named[22529]: error (unexpected RCODE REFUSED) resolving 'slack-imgs.com/A/IN': 95.110.64.205#53",
            "Sep  8 11:55:35 linux named[22529]: error (unexpected RCODE REFUSED) resolving 'www.elastic.co/A/IN': 95.110.68.206#53",
            "Sep  8 11:55:42 linux named[22529]: error (unexpected RCODE REFUSED) resolving 'b.akamaiedge.net/A/IN': 95.110.64.205#53"
        );

        Map<String, Object> mappings = new HashMap<>();
        GrokPatternCreator[] grokPatternCreators = new GrokPatternCreator[] {
            new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            ),
            new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_V1
            ) };

        String[] expectedLoglevel = new String[] { "loglevel", "log.level" };

        int index = 0;
        for (GrokPatternCreator grokPatternCreator : grokPatternCreators) {
            String loglevel = expectedLoglevel[index];
            assertEquals(
                "%{SYSLOGTIMESTAMP:timestamp} .*? .*?\\[%{INT:field}\\]: %{LOGLEVEL:"
                    + loglevel
                    + "} \\(.*? .*? .*?\\) .*? "
                    + "%{QUOTEDSTRING:field2}: %{IP:ipaddress}#%{INT:field3}",
                grokPatternCreator.createGrokPatternFromExamples(
                    "SYSLOGTIMESTAMP",
                    TextStructureUtils.DATE_MAPPING_WITHOUT_FORMAT,
                    "timestamp"
                )
            );
            assertEquals(5, mappings.size());
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long"), mappings.get("field"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get(loglevel));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("field2"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "ip"), mappings.get("ipaddress"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long"), mappings.get("field3"));

            mappings.clear();
            ++index;
        }
    }

    public void testCreateGrokPatternFromExamplesGivenCatalinaLogs() {

        Collection<String> sampleMessages = Arrays.asList(
            "Aug 29, 2009 12:03:33 AM org.apache.tomcat.util.http.Parameters processParameters\nWARNING: Parameters: "
                + "Invalid chunk ignored.",
            "Aug 29, 2009 12:03:40 AM org.apache.tomcat.util.http.Parameters processParameters\nWARNING: Parameters: "
                + "Invalid chunk ignored.",
            "Aug 29, 2009 12:03:45 AM org.apache.tomcat.util.http.Parameters processParameters\nWARNING: Parameters: "
                + "Invalid chunk ignored.",
            "Aug 29, 2009 12:03:57 AM org.apache.tomcat.util.http.Parameters processParameters\nWARNING: Parameters: "
                + "Invalid chunk ignored."
        );

        Map<String, Object> mappings = new HashMap<>();
        GrokPatternCreator[] grokPatternCreators = new GrokPatternCreator[] {
            new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            ),
            new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_V1
            ) };

        String[] expectedLoglevel = new String[] { "loglevel", "log.level" };

        int index = 0;
        for (GrokPatternCreator grokPatternCreator : grokPatternCreators) {
            String loglevel = expectedLoglevel[index];
            assertEquals(
                "%{CATALINA_DATESTAMP:timestamp} .*? .*?\\n%{LOGLEVEL:" + loglevel + "}: .*",
                grokPatternCreator.createGrokPatternFromExamples(
                    "CATALINA_DATESTAMP",
                    TextStructureUtils.DATE_MAPPING_WITHOUT_FORMAT,
                    "timestamp"
                )
            );
            assertEquals(1, mappings.size());
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get(loglevel));

            mappings.clear();
            ++index;
        }

        {
            // ECS compatible Grok also supports the new CATALINA7_DATESTAMP pattern name
            GrokPatternCreator grokPatternCreator = new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_V1
            );
            assertEquals(
                "%{CATALINA7_DATESTAMP:timestamp} .*? .*?\\n%{LOGLEVEL:log.level}: .*",
                grokPatternCreator.createGrokPatternFromExamples(
                    "CATALINA7_DATESTAMP",
                    TextStructureUtils.DATE_MAPPING_WITHOUT_FORMAT,
                    "timestamp"
                )
            );
            assertEquals(1, mappings.size());
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("log.level"));
        }

    }

    public void testCreateGrokPatternFromExamplesGivenCatalina8Logs() {

        // Tomcat 8.5/9.0 has new datestamp and logging formats - these are only supported by ECS compatible Grok patterns, i.e.:
        // CATALINA8_DATESTAMP %{MONTHDAY}-%{MONTH}-%{YEAR} %{HOUR}:%{MINUTE}:%{SECOND}
        // CATALINA8_LOG %{CATALINA8_DATESTAMP:timestamp} %{LOGLEVEL:log.level} \[%{DATA:java.log.origin.thread.name}\]
        // %{JAVACLASS:java.log.origin.class.name}\.(?:%{JAVAMETHOD:log.origin.function})? %{JAVALOGMESSAGE:message}
        // CATALINA_DATESTAMP (?:%{CATALINA8_DATESTAMP})|(?:%{CATALINA7_DATESTAMP})
        // CATALINALOG (?:%{CATALINA8_LOG})|(?:%{CATALINA7_LOG})
        Collection<String> sampleMessages = Arrays.asList(
            "29-Aug-2009 12:03:33.123 WARNING [main] org.apache.tomcat.util.http.Parameters.processParameters Parameters: "
                + "Invalid chunk ignored.",
            "29-Aug-2009 12:03:40.234 WARNING [main] org.apache.tomcat.util.http.Parameters.processParameters Parameters: "
                + "Invalid chunk ignored.",
            "29-Aug-2009 12:03:45.456 WARNING [main] org.apache.tomcat.util.http.Parameters.processParameters Parameters: "
                + "Invalid chunk ignored.",
            "29-Aug-2009 12:03:57.567 WARNING [main] org.apache.tomcat.util.http.Parameters.processParameters Parameters: "
                + "Invalid chunk ignored."
        );

        {
            Map<String, Object> mappings = new HashMap<>();
            GrokPatternCreator grokPatternCreator = new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            );

            // Non ECS compatible Grok patterns will fail to match the new CATALINA_DATESTAMP format
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> grokPatternCreator.createGrokPatternFromExamples(
                    "CATALINA_DATESTAMP",
                    TextStructureUtils.DATE_MAPPING_WITHOUT_FORMAT,
                    "timestamp"
                )
            );

            assertEquals(
                "[%{CATALINA_DATESTAMP}] does not match snippet [29-Aug-2009 12:03:33.123 WARNING [main]"
                    + " org.apache.tomcat.util.http.Parameters.processParameters Parameters: Invalid chunk ignored.]",
                e.getMessage()
            );
        }
        {
            Map<String, Object> mappings = new HashMap<>();
            GrokPatternCreator grokPatternCreator = new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_V1
            );

            // ECS compatible Grok patterns match the new, generic CATALINA_DATESTAMP format and provide the new, ECS name for the LOGLEVEL
            // capture.
            assertEquals(
                "%{CATALINA_DATESTAMP:timestamp} %{LOGLEVEL:log.level} \\[.*",
                grokPatternCreator.createGrokPatternFromExamples(
                    "CATALINA_DATESTAMP",
                    TextStructureUtils.DATE_MAPPING_WITHOUT_FORMAT,
                    "timestamp"
                )
            );
            assertEquals(1, mappings.size());
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("log.level"));
        }
        {
            Map<String, Object> mappings = new HashMap<>();
            GrokPatternCreator grokPatternCreator = new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_V1
            );

            // ECS compatible Grok patterns also understand the new CATALINA8_DATESTAMP Grok pattern name and provide the new, ECS name for
            // the LOGLEVEL capture.
            assertEquals(
                "%{CATALINA8_DATESTAMP:timestamp} %{LOGLEVEL:log.level} \\[.*",
                grokPatternCreator.createGrokPatternFromExamples(
                    "CATALINA8_DATESTAMP",
                    TextStructureUtils.DATE_MAPPING_WITHOUT_FORMAT,
                    "timestamp"
                )
            );
            assertEquals(1, mappings.size());
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("log.level"));
        }
    }

    public void testCreateGrokPatternFromExamplesGivenMultiTimestampLogs() {

        // Two timestamps: one local, one UTC
        Collection<String> sampleMessages = Arrays.asList(
            "559550912540598297\t2016-04-20T14:06:53\t2016-04-20T21:06:53Z\t38545844\tserv02nw07\t192.168.114.28\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp",
            "559550912548986880\t2016-04-20T14:06:53\t2016-04-20T21:06:53Z\t9049724\tserv02nw03\t10.120.48.147\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp",
            "559550912548986887\t2016-04-20T14:06:53\t2016-04-20T21:06:53Z\t884343\tserv02tw03\t192.168.121.189\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp",
            "559550912603512850\t2016-04-20T14:06:53\t2016-04-20T21:06:53Z\t8907014\tserv02nw01\t192.168.118.208\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp"
        );

        Map<String, Object> mappings = new HashMap<>();

        GrokPatternCreator[] grokPatternCreators = new GrokPatternCreator[] {
            new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            ),
            new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_V1
            ) };

        String[] expectedLoglevel = new String[] { "loglevel", "log.level" };

        int index = 0;
        for (GrokPatternCreator grokPatternCreator : grokPatternCreators) {
            String loglevel = expectedLoglevel[index];
            assertEquals(
                "%{INT:field}\\t%{TIMESTAMP_ISO8601:timestamp}\\t%{TIMESTAMP_ISO8601:extra_timestamp}\\t%{INT:field2}\\t.*?\\t"
                    + "%{IP:ipaddress}\\t.*?\\t%{LOGLEVEL:"
                    + loglevel
                    + "}\\t.*",
                grokPatternCreator.createGrokPatternFromExamples(
                    "TIMESTAMP_ISO8601",
                    TextStructureUtils.DATE_MAPPING_WITHOUT_FORMAT,
                    "timestamp"
                )
            );
            assertEquals(5, mappings.size());
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long"), mappings.get("field"));
            Map<String, String> expectedDateMapping = new HashMap<>();
            expectedDateMapping.put(TextStructureUtils.MAPPING_TYPE_SETTING, "date");
            expectedDateMapping.put(TextStructureUtils.MAPPING_FORMAT_SETTING, "iso8601");
            assertEquals(expectedDateMapping, mappings.get("extra_timestamp"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long"), mappings.get("field2"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "ip"), mappings.get("ipaddress"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get(loglevel));

            mappings.clear();
            ++index;
        }
    }

    public void testCreateGrokPatternFromExamplesGivenMultiTimestampLogsAndIndeterminateFormat() {

        // Two timestamps: one ISO8601, one indeterminate day/month
        Collection<String> sampleMessages = Arrays.asList(
            "559550912540598297\t2016-04-20T14:06:53\t20/04/2016 21:06:53,123456\t38545844\tserv02nw07\t192.168.114.28\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp",
            "559550912548986880\t2016-04-20T14:06:53\t20/04/2016 21:06:53,123456\t9049724\tserv02nw03\t10.120.48.147\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp",
            "559550912548986887\t2016-04-20T14:06:53\t20/04/2016 21:06:53,123456\t884343\tserv02tw03\t192.168.121.189\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp",
            "559550912603512850\t2016-04-20T14:06:53\t20/04/2016 21:06:53,123456\t8907014\tserv02nw01\t192.168.118.208\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp"
        );

        Map<String, Object> mappings = new HashMap<>();

        GrokPatternCreator[] grokPatternCreators = new GrokPatternCreator[] {
            new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            ),
            new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_V1
            ) };

        String[] expectedLoglevel = new String[] { "loglevel", "log.level" };

        int index = 0;
        for (GrokPatternCreator grokPatternCreator : grokPatternCreators) {
            String loglevel = expectedLoglevel[index];
            assertEquals(
                "%{INT:field}\\t%{TIMESTAMP_ISO8601:timestamp}\\t%{DATESTAMP:extra_timestamp}\\t%{INT:field2}\\t.*?\\t"
                    + "%{IP:ipaddress}\\t.*?\\t%{LOGLEVEL:"
                    + loglevel
                    + "}\\t.*",
                grokPatternCreator.createGrokPatternFromExamples(
                    "TIMESTAMP_ISO8601",
                    TextStructureUtils.DATE_MAPPING_WITHOUT_FORMAT,
                    "timestamp"
                )
            );
            assertEquals(5, mappings.size());
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long"), mappings.get("field"));
            Map<String, String> expectedDateMapping = new HashMap<>();
            expectedDateMapping.put(TextStructureUtils.MAPPING_TYPE_SETTING, "date_nanos");
            expectedDateMapping.put(TextStructureUtils.MAPPING_FORMAT_SETTING, "dd/MM/yyyy HH:mm:ss,SSSSSS");
            assertEquals(expectedDateMapping, mappings.get("extra_timestamp"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long"), mappings.get("field2"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "ip"), mappings.get("ipaddress"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get(loglevel));

            mappings.clear();
            ++index;
        }
    }

    public void testCreateGrokPatternFromExamplesGivenMultiTimestampLogsAndCustomDefinition() {

        // Two timestamps: one custom, one built-in
        Collection<String> sampleMessages = Arrays.asList(
            "559550912540598297\t4/20/2016 2:06PM\t2016-04-20T21:06:53Z\t38545844\tserv02nw07\t192.168.114.28\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp",
            "559550912548986880\t4/20/2016 2:06PM\t2016-04-20T21:06:53Z\t9049724\tserv02nw03\t10.120.48.147\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp",
            "559550912548986887\t4/20/2016 2:06PM\t2016-04-20T21:06:53Z\t884343\tserv02tw03\t192.168.121.189\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp",
            "559550912603512850\t4/20/2016 2:06PM\t2016-04-20T21:06:53Z\t8907014\tserv02nw01\t192.168.118.208\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp"
        );

        Map<String, Object> mappings = new HashMap<>();

        GrokPatternCreator[] grokPatternCreators = new GrokPatternCreator[] {
            new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.singletonMap("CUSTOM_TIMESTAMP", "%{MONTHNUM}/%{MONTHDAY}/%{YEAR} %{HOUR}:%{MINUTE}(?:AM|PM)"),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            ),
            new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.singletonMap("CUSTOM_TIMESTAMP", "%{MONTHNUM}/%{MONTHDAY}/%{YEAR} %{HOUR}:%{MINUTE}(?:AM|PM)"),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_V1
            ) };

        String[] expectedLoglevel = new String[] { "loglevel", "log.level" };

        int index = 0;
        for (GrokPatternCreator grokPatternCreator : grokPatternCreators) {
            String loglevel = expectedLoglevel[index];
            Map<String, String> customMapping = new HashMap<>();
            customMapping.put(TextStructureUtils.MAPPING_TYPE_SETTING, "date");
            customMapping.put(TextStructureUtils.MAPPING_FORMAT_SETTING, "M/dd/yyyy h:mma");
            assertEquals(
                "%{INT:field}\\t%{CUSTOM_TIMESTAMP:timestamp}\\t%{TIMESTAMP_ISO8601:extra_timestamp}\\t%{INT:field2}\\t.*?\\t"
                    + "%{IP:ipaddress}\\t.*?\\t%{LOGLEVEL:"
                    + loglevel
                    + "}\\t.*",
                grokPatternCreator.createGrokPatternFromExamples("CUSTOM_TIMESTAMP", customMapping, "timestamp")
            );
            assertEquals(5, mappings.size());
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long"), mappings.get("field"));
            Map<String, String> expectedDateMapping = new HashMap<>();
            expectedDateMapping.put(TextStructureUtils.MAPPING_TYPE_SETTING, "date");
            expectedDateMapping.put(TextStructureUtils.MAPPING_FORMAT_SETTING, "iso8601");
            assertEquals(expectedDateMapping, mappings.get("extra_timestamp"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long"), mappings.get("field2"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "ip"), mappings.get("ipaddress"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get(loglevel));

            mappings.clear();
            ++index;
        }
    }

    public void testCreateGrokPatternFromExamplesGivenTimestampAndTimeWithoutDate() {

        // Two timestamps: one with date, one without
        Collection<String> sampleMessages = Arrays.asList(
            "559550912540598297\t2016-04-20T14:06:53\t21:06:53.123456\t38545844\tserv02nw07\t192.168.114.28\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp",
            "559550912548986880\t2016-04-20T14:06:53\t21:06:53.123456\t9049724\tserv02nw03\t10.120.48.147\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp",
            "559550912548986887\t2016-04-20T14:06:53\t21:06:53.123456\t884343\tserv02tw03\t192.168.121.189\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp",
            "559550912603512850\t2016-04-20T14:06:53\t21:06:53.123456\t8907014\tserv02nw01\t192.168.118.208\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp"
        );

        Map<String, Object> mappings = new HashMap<>();

        GrokPatternCreator[] grokPatternCreators = new GrokPatternCreator[] {
            new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            ),
            new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_V1
            ) };

        String[] expectedLoglevel = new String[] { "loglevel", "log.level" };

        int index = 0;
        for (GrokPatternCreator grokPatternCreator : grokPatternCreators) {
            String loglevel = expectedLoglevel[index];
            assertEquals(
                "%{INT:field}\\t%{TIMESTAMP_ISO8601:timestamp}\\t%{TIME:time}\\t%{INT:field2}\\t.*?\\t"
                    + "%{IP:ipaddress}\\t.*?\\t%{LOGLEVEL:"
                    + loglevel
                    + "}\\t.*",
                grokPatternCreator.createGrokPatternFromExamples(
                    "TIMESTAMP_ISO8601",
                    TextStructureUtils.DATE_MAPPING_WITHOUT_FORMAT,
                    "timestamp"
                )
            );
            assertEquals(5, mappings.size());
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long"), mappings.get("field"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("time"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long"), mappings.get("field2"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "ip"), mappings.get("ipaddress"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get(loglevel));

            mappings.clear();
            ++index;
        }
    }

    public void testFindFullLineGrokPatternGivenApacheCombinedLogs() {
        Collection<String> sampleMessages = Arrays.asList(
            "83.149.9.216 - - [19/Jan/2016:08:13:42 +0000] "
                + "\"GET /presentations/logstash-monitorama-2013/images/kibana-search.png HTTP/1.1\" 200 203023 "
                + "\"http://semicomplete.com/presentations/logstash-monitorama-2013/\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) "
                + "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.77 Safari/537.36\"",
            "83.149.9.216 - - [19/Jan/2016:08:13:44 +0000] "
                + "\"GET /presentations/logstash-monitorama-2013/plugin/zoom-js/zoom.js HTTP/1.1\" 200 7697 "
                + "\"http://semicomplete.com/presentations/logstash-monitorama-2013/\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) "
                + "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.77 Safari/537.36\"",
            "83.149.9.216 - - [19/Jan/2016:08:13:44 +0000] "
                + "\"GET /presentations/logstash-monitorama-2013/plugin/highlight/highlight.js HTTP/1.1\" 200 26185 "
                + "\"http://semicomplete.com/presentations/logstash-monitorama-2013/\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) "
                + "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.77 Safari/537.36\"",
            "83.149.9.216 - - [19/Jan/2016:08:13:42 +0000] "
                + "\"GET /presentations/logstash-monitorama-2013/images/sad-medic.png HTTP/1.1\" 200 430406 "
                + "\"http://semicomplete.com/presentations/logstash-monitorama-2013/\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) "
                + "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.77 Safari/537.36\""
        );

        Map<String, Object> mappings = new HashMap<>();

        GrokPatternCreator[] grokPatternCreators = new GrokPatternCreator[] {
            new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            ),
            new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_V1
            ) };

        {
            assertEquals(
                new Tuple<>("timestamp", "%{COMBINEDAPACHELOG}"),
                grokPatternCreators[0].findFullLineGrokPattern(randomBoolean() ? "timestamp" : null)
            );
            assertEquals(10, mappings.size());
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "text"), mappings.get("agent"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("auth"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long"), mappings.get("bytes"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "ip"), mappings.get("clientip"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "double"), mappings.get("httpversion"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("ident"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("referrer"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("request"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long"), mappings.get("response"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("verb"));

            mappings.clear();
        }
        {
            // ECS Grok patterns for httpd logs have markedly different capture names from the legacy ones
            assertEquals(
                new Tuple<>("timestamp", "%{COMBINEDAPACHELOG}"),
                grokPatternCreators[1].findFullLineGrokPattern(randomBoolean() ? "timestamp" : null)
            );
            assertEquals(8, mappings.size());
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "text"), mappings.get("user_agent.original"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("url.original"));
            assertEquals(
                Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long"),
                mappings.get("http.response.body.bytes")
            );
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "ip"), mappings.get("source.address"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "double"), mappings.get("http.version"));
            assertEquals(
                Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"),
                mappings.get("http.request.referrer")
            );
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("http.request.method"));
            assertEquals(
                Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long"),
                mappings.get("http.response.status_code")
            );

            mappings.clear();
        }
    }

    public void testAdjustForPunctuationGivenCommonPrefix() {
        Collection<String> snippets = """
            ","lab6.localhost","Route Domain","/Common/0","No-lookup","192.168.33.212","No-lookup","192.168.33.132","80","46721",\
            "/Common/Subnet_33","TCP","0","","","","","","","","Staged","/Common/policy1","rule1","Accept","","","",\
            "0000000000000000"
            ","lab6.localhost","Route Domain","/Common/0","No-lookup","192.168.143.244","No-lookup","192.168.33.106","55025","162",\
            "/Common/Subnet_33","UDP","0","","","","","","","","Staged","/Common/policy1","rule1","Accept","","","",\
            "0000000000000000"
            ","lab6.localhost","Route Domain","/Common/0","No-lookup","192.168.33.3","No-lookup","224.0.0.102","3222","3222",\
            "/Common/Subnet_33","UDP","0","","","","","","","","Staged","/Common/policy1","rule1","Accept","","","",\
            "0000000000000000"\
            """.lines().toList();

        GrokPatternCreator[] grokPatternCreators = new GrokPatternCreator[] {
            new GrokPatternCreator(
                explanation,
                snippets,
                null,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            ),
            new GrokPatternCreator(explanation, snippets, null, null, Collections.emptyMap(), NOOP_TIMEOUT_CHECKER, ECS_COMPATIBILITY_V1) };

        for (GrokPatternCreator grokPatternCreator : grokPatternCreators) {
            Collection<String> adjustedSnippets = grokPatternCreator.adjustForPunctuation(snippets);

            assertEquals("\",", grokPatternCreator.getOverallGrokPatternBuilder().toString());
            assertNotNull(adjustedSnippets);
            assertThat(
                new ArrayList<>(adjustedSnippets),
                containsInAnyOrder(snippets.stream().map(snippet -> snippet.substring(2)).toArray(String[]::new))
            );
        }
    }

    public void testAdjustForPunctuationGivenNoCommonPrefix() {
        Collection<String> snippets = Arrays.asList(
            "|client (id:2) was removed from servergroup 'Normal'(id:7) by client 'User1'(id:2)",
            "|servergroup 'GAME'(id:9) was added by 'User1'(id:2)",
            "|permission 'i_group_auto_update_type'(id:146) with values (value:30, negated:0, skipchannel:0) "
                + "was added by 'User1'(id:2) to servergroup 'GAME'(id:9)"
        );

        GrokPatternCreator[] grokPatternCreators = new GrokPatternCreator[] {
            new GrokPatternCreator(
                explanation,
                snippets,
                null,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            ),
            new GrokPatternCreator(explanation, snippets, null, null, Collections.emptyMap(), NOOP_TIMEOUT_CHECKER, ECS_COMPATIBILITY_V1) };

        for (GrokPatternCreator grokPatternCreator : grokPatternCreators) {
            Collection<String> adjustedSnippets = grokPatternCreator.adjustForPunctuation(snippets);

            assertEquals("", grokPatternCreator.getOverallGrokPatternBuilder().toString());
            assertSame(snippets, adjustedSnippets);
        }
    }

    public void testValidateFullLineGrokPatternGivenValid() {

        String timestampField = "utc_timestamp";
        String grokPattern = "%{INT:serial_no}\\t%{TIMESTAMP_ISO8601:local_timestamp}\\t%{TIMESTAMP_ISO8601:utc_timestamp}\\t"
            + "%{INT:user_id}\\t%{HOSTNAME:host}\\t%{IP:client_ip}\\t%{WORD:method}\\t%{LOGLEVEL:severity}\\t%{PROG:program}\\t"
            + "%{GREEDYDATA:message}";

        // Two timestamps: one local, one UTC
        Collection<String> sampleMessages = Arrays.asList(
            "559550912540598297\t2016-04-20T14:06:53\t2016-04-20T21:06:53Z\t38545844\tserv02nw07\t192.168.114.28\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp",
            "559550912548986880\t2016-04-20T14:06:53\t2016-04-20T21:06:53Z\t9049724\tserv02nw03\t10.120.48.147\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp",
            "559550912548986887\t2016-04-20T14:06:53\t2016-04-20T21:06:53Z\t884343\tserv02tw03\t192.168.121.189\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp",
            "559550912603512850\t2016-04-20T14:06:53\t2016-04-20T21:06:53Z\t8907014\tserv02nw01\t192.168.118.208\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp"
        );

        Map<String, Object> mappings = new HashMap<>();

        GrokPatternCreator[] grokPatternCreators = new GrokPatternCreator[] {
            new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            ),
            new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_V1
            ) };

        for (GrokPatternCreator grokPatternCreator : grokPatternCreators) {
            grokPatternCreator.validateFullLineGrokPattern(grokPattern, timestampField);
            assertEquals(9, mappings.size());
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long"), mappings.get("serial_no"));
            Map<String, String> expectedDateMapping = new HashMap<>();
            expectedDateMapping.put(TextStructureUtils.MAPPING_TYPE_SETTING, "date");
            expectedDateMapping.put(TextStructureUtils.MAPPING_FORMAT_SETTING, "iso8601");
            assertEquals(expectedDateMapping, mappings.get("local_timestamp"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long"), mappings.get("user_id"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("host"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "ip"), mappings.get("client_ip"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("method"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("severity"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("program"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("message"));

            mappings.clear();
        }
    }

    public void testValidateFullLineGrokPatternGivenValidAndCustomDefinition() {

        String timestampField = "local_timestamp";
        String grokPattern = "%{INT:serial_no}\\t%{CUSTOM_TIMESTAMP:local_timestamp}\\t%{TIMESTAMP_ISO8601:utc_timestamp}\\t"
            + "%{INT:user_id}\\t%{HOSTNAME:host}\\t%{IP:client_ip}\\t%{WORD:method}\\t%{LOGLEVEL:severity}\\t%{PROG:program}\\t"
            + "%{GREEDYDATA:message}";

        // Two timestamps: one local, one UTC
        Collection<String> sampleMessages = Arrays.asList(
            "559550912540598297\t4/20/2016 2:06PM\t2016-04-20T21:06:53Z\t38545844\tserv02nw07\t192.168.114.28\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp",
            "559550912548986880\t4/20/2016 2:06PM\t2016-04-20T21:06:53Z\t9049724\tserv02nw03\t10.120.48.147\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp",
            "559550912548986887\t4/20/2016 2:06PM\t2016-04-20T21:06:53Z\t884343\tserv02tw03\t192.168.121.189\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp",
            "559550912603512850\t4/20/2016 2:06PM\t2016-04-20T21:06:53Z\t8907014\tserv02nw01\t192.168.118.208\tAuthpriv\t"
                + "Info\tsshd\tsubsystem request for sftp"
        );

        Map<String, Object> mappings = new HashMap<>();

        GrokPatternCreator[] grokPatternCreators = new GrokPatternCreator[] {
            new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.singletonMap("CUSTOM_TIMESTAMP", "%{MONTHNUM}/%{MONTHDAY}/%{YEAR} %{HOUR}:%{MINUTE}(?:AM|PM)"),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            ),
            new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.singletonMap("CUSTOM_TIMESTAMP", "%{MONTHNUM}/%{MONTHDAY}/%{YEAR} %{HOUR}:%{MINUTE}(?:AM|PM)"),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_V1
            ) };

        for (GrokPatternCreator grokPatternCreator : grokPatternCreators) {
            grokPatternCreator.validateFullLineGrokPattern(grokPattern, timestampField);
            assertEquals(9, mappings.size());
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long"), mappings.get("serial_no"));
            Map<String, String> expectedDateMapping = new HashMap<>();
            expectedDateMapping.put(TextStructureUtils.MAPPING_TYPE_SETTING, "date");
            expectedDateMapping.put(TextStructureUtils.MAPPING_FORMAT_SETTING, "iso8601");
            assertEquals(expectedDateMapping, mappings.get("utc_timestamp"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long"), mappings.get("user_id"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("host"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "ip"), mappings.get("client_ip"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("method"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("severity"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("program"));
            assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("message"));

            mappings.clear();
        }
    }

    public void testValidateFullLineGrokPatternGivenInvalid() {

        String timestampField = "utc_timestamp";
        String grokPattern = "%{INT:serial_no}\\t%{TIMESTAMP_ISO8601:local_timestamp}\\t%{TIMESTAMP_ISO8601:utc_timestamp}\\t"
            + "%{INT:user_id}\\t%{HOSTNAME:host}\\t%{IP:client_ip}\\t%{WORD:method}\\t%{LOGLEVEL:severity}\\t%{PROG:program}\\t"
            + "%{GREEDYDATA:message}";

        Collection<String> sampleMessages = Arrays.asList(
            "Sep  8 11:55:06 linux named[22529]: error (unexpected RCODE REFUSED) resolving 'elastic.slack.com/A/IN': 95.110.64.205#53",
            "Sep  8 11:55:08 linux named[22529]: error (unexpected RCODE REFUSED) resolving 'slack-imgs.com/A/IN': 95.110.64.205#53",
            "Sep  8 11:55:35 linux named[22529]: error (unexpected RCODE REFUSED) resolving 'www.elastic.co/A/IN': 95.110.68.206#53",
            "Sep  8 11:55:42 linux named[22529]: error (unexpected RCODE REFUSED) resolving 'b.akamaiedge.net/A/IN': 95.110.64.205#53"
        );

        Map<String, Object> mappings = new HashMap<>();

        GrokPatternCreator[] grokPatternCreators = new GrokPatternCreator[] {
            new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_DISABLED
            ),
            new GrokPatternCreator(
                explanation,
                sampleMessages,
                mappings,
                null,
                Collections.emptyMap(),
                NOOP_TIMEOUT_CHECKER,
                ECS_COMPATIBILITY_V1
            ) };

        for (GrokPatternCreator grokPatternCreator : grokPatternCreators) {

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> grokPatternCreator.validateFullLineGrokPattern(grokPattern, timestampField)
            );

            assertEquals("Supplied Grok pattern [" + grokPattern + "] does not match sample messages", e.getMessage());

            mappings.clear();
        }
    }

    public void testLongestRun() {

        List<Integer> sequence = new ArrayList<>();
        if (randomBoolean()) {
            for (int before = randomIntBetween(1, 41); before > 0; --before) {
                sequence.add(randomIntBetween(1, 2));
            }
        }
        for (int longest = 42; longest > 0; --longest) {
            sequence.add(42);
        }
        if (randomBoolean()) {
            for (int after = randomIntBetween(1, 41); after > 0; --after) {
                sequence.add(randomIntBetween(2, 3));
            }
        }
        assertThat(GrokPatternCreator.longestRun(sequence), is(42));
    }
}
