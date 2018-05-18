/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.configcreator.GrokPatternCreator.GrokPatternCandidate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class GrokPatternCreatorTests extends ESTestCase {

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

        Collection<String> matchingStrings = Arrays.asList("[2018-01-25T15:33:23] DEBUG ",
            "[2018-01-24T12:33:23] ERROR ",
            "junk [2018-01-22T07:33:23] INFO ",
            "[2018-01-21T03:33:23] DEBUG ");
        GrokPatternCandidate candidate = new GrokPatternCandidate("TIMESTAMP_ISO8601", "date", "extra_timestamp");
        Collection<String> prefaces = new ArrayList<>();
        Collection<String> epilogues = new ArrayList<>();

        GrokPatternCreator.populatePrefacesAndEpilogues(matchingStrings, candidate.grokPatternName, candidate.grok, prefaces, epilogues);

        assertThat(prefaces, containsInAnyOrder("[", "[", "junk [", "["));
        assertThat(epilogues, containsInAnyOrder("] DEBUG ", "] ERROR ", "] INFO ", "] DEBUG "));
    }

    public void testPopulatePrefacesAndEpiloguesGivenEmailAddress() {

        Collection<String> matchingStrings = Arrays.asList("before alice@acme.com after",
            "abc bob@acme.com xyz",
            "carol@acme.com");
        GrokPatternCandidate candidate = new GrokPatternCandidate("EMAILADDRESS", "keyword", "email");
        Collection<String> prefaces = new ArrayList<>();
        Collection<String> epilogues = new ArrayList<>();

        GrokPatternCreator.populatePrefacesAndEpilogues(matchingStrings, candidate.grokPatternName, candidate.grok, prefaces, epilogues);

        assertThat(prefaces, containsInAnyOrder("before ", "abc ", ""));
        assertThat(epilogues, containsInAnyOrder(" after", " xyz", ""));
    }

    public void testAppendBestGrokMatchForStringsGivenTimestampsAndLogLevels() {

        Collection<String> snippets = Arrays.asList("[2018-01-25T15:33:23] DEBUG ",
            "[2018-01-24T12:33:23] ERROR ",
            "junk [2018-01-22T07:33:23] INFO ",
            "[2018-01-21T03:33:23] DEBUG ");

        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        StringBuilder overallGrokPatternBuilder = new StringBuilder();
        Map<String, String> mappings = new HashMap<>();

        GrokPatternCreator.appendBestGrokMatchForStrings(fieldNameCountStore, overallGrokPatternBuilder, false, snippets, mappings);

        assertEquals(".*?\\[%{TIMESTAMP_ISO8601:extra_timestamp}\\] %{LOGLEVEL:loglevel} ", overallGrokPatternBuilder.toString());
    }

    public void testAppendBestGrokMatchForStringsGivenNumbersInBrackets() {

        Collection<String> snippets = Arrays.asList("(-2)",
            "  (-3)",
            " (4)",
            " (-5) ");

        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        StringBuilder overallGrokPatternBuilder = new StringBuilder();
        Map<String, String> mappings = new HashMap<>();

        GrokPatternCreator.appendBestGrokMatchForStrings(fieldNameCountStore, overallGrokPatternBuilder, false, snippets, mappings);

        assertEquals(".*?\\(%{INT:field}\\).*?", overallGrokPatternBuilder.toString());
    }

    public void testAppendBestGrokMatchForStringsGivenNegativeNumbersWithoutBreak() {

        Collection<String> snippets = Arrays.asList("before-2 ",
            "prior to-3",
            "-4");

        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        StringBuilder overallGrokPatternBuilder = new StringBuilder();
        Map<String, String> mappings = new HashMap<>();

        GrokPatternCreator.appendBestGrokMatchForStrings(fieldNameCountStore, overallGrokPatternBuilder, false, snippets, mappings);

        // It seems sensible that we don't detect these suffices as either base 10 or base 16 numbers
        assertEquals(".*?", overallGrokPatternBuilder.toString());
    }

    public void testAppendBestGrokMatchForStringsGivenHexNumbers() {

        Collection<String> snippets = Arrays.asList(" abc",
            "  123",
            " -123",
            "1f is hex");

        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        StringBuilder overallGrokPatternBuilder = new StringBuilder();
        Map<String, String> mappings = new HashMap<>();

        GrokPatternCreator.appendBestGrokMatchForStrings(fieldNameCountStore, overallGrokPatternBuilder, false, snippets, mappings);

        assertEquals(".*?%{BASE16NUM:field}.*?", overallGrokPatternBuilder.toString());
    }

    public void testAppendBestGrokMatchForStringsGivenHostnamesWithNumbers() {

        Collection<String> snippets = Arrays.asList("<host1.1.p2ps:",
            "<host2.1.p2ps:");

        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        StringBuilder overallGrokPatternBuilder = new StringBuilder();
        Map<String, String> mappings = new HashMap<>();

        GrokPatternCreator.appendBestGrokMatchForStrings(fieldNameCountStore, overallGrokPatternBuilder, false, snippets, mappings);

        // We don't want the .1. in the middle to get detected as a hex number
        assertEquals("<.*?:", overallGrokPatternBuilder.toString());
    }

    public void testAppendBestGrokMatchForStringsGivenEmailAddresses() {

        Collection<String> snippets = Arrays.asList("before alice@acme.com after",
            "abc bob@acme.com xyz",
            "carol@acme.com");

        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        StringBuilder overallGrokPatternBuilder = new StringBuilder();
        Map<String, String> mappings = new HashMap<>();

        GrokPatternCreator.appendBestGrokMatchForStrings(fieldNameCountStore, overallGrokPatternBuilder, false, snippets, mappings);

        assertEquals(".*?%{EMAILADDRESS:email}.*?", overallGrokPatternBuilder.toString());
    }

    public void testAppendBestGrokMatchForStringsGivenUris() {

        Collection<String> snippets = Arrays.asList("main site https://www.elastic.co/ with trailing slash",
            "https://www.elastic.co/guide/en/x-pack/current/ml-configuring-categories.html#ml-configuring-categories is a section",
            "download today from https://www.elastic.co/downloads");

        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        StringBuilder overallGrokPatternBuilder = new StringBuilder();
        Map<String, String> mappings = new HashMap<>();

        GrokPatternCreator.appendBestGrokMatchForStrings(fieldNameCountStore, overallGrokPatternBuilder, false, snippets, mappings);

        assertEquals(".*?%{URI:uri}.*?", overallGrokPatternBuilder.toString());
    }

    public void testAppendBestGrokMatchForStringsGivenPaths() {

        Collection<String> snippets = Arrays.asList("on Mac /Users/dave",
            "on Windows C:\\Users\\dave",
            "on Linux /home/dave");

        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        StringBuilder overallGrokPatternBuilder = new StringBuilder();
        Map<String, String> mappings = new HashMap<>();

        GrokPatternCreator.appendBestGrokMatchForStrings(fieldNameCountStore, overallGrokPatternBuilder, false, snippets, mappings);

        assertEquals(".*? .*? %{PATH:path}", overallGrokPatternBuilder.toString());
    }

    public void testCreateGrokPatternFromExamplesGivenNamedLogs() {

        Collection<String> sampleMessages = Arrays.asList(
            "Sep  8 11:55:06 linux named[22529]: error (unexpected RCODE REFUSED) resolving 'elastic.slack.com/A/IN': 95.110.64.205#53",
            "Sep  8 11:55:08 linux named[22529]: error (unexpected RCODE REFUSED) resolving 'slack-imgs.com/A/IN': 95.110.64.205#53",
            "Sep  8 11:55:35 linux named[22529]: error (unexpected RCODE REFUSED) resolving 'www.elastic.co/A/IN': 95.110.68.206#53",
            "Sep  8 11:55:42 linux named[22529]: error (unexpected RCODE REFUSED) resolving 'b.akamaiedge.net/A/IN': 95.110.64.205#53");
        Map<String, String> mappings = new HashMap<>();

        assertEquals("%{SYSLOGTIMESTAMP:_timestamp} .*? .*?\\[%{INT:field}\\]: %{LOGLEVEL:loglevel} \\(.*? .*? .*?\\) .*? " +
                "%{QUOTEDSTRING:field2}: %{IP:ipaddress}#%{INT:field3}",
            GrokPatternCreator.createGrokPatternFromExamples(sampleMessages, "SYSLOGTIMESTAMP", "_timestamp", mappings));
        assertEquals(5, mappings.size());
        assertEquals("long", mappings.get("field"));
        assertEquals("keyword", mappings.get("loglevel"));
        assertEquals("keyword", mappings.get("field2"));
        assertEquals("ip", mappings.get("ipaddress"));
        assertEquals("long", mappings.get("field3"));
    }

    public void testCreateGrokPatternFromExamplesGivenCatalinaLogs() {

        Collection<String> sampleMessages = Arrays.asList(
            "Aug 29, 2009 12:03:33 AM org.apache.tomcat.util.http.Parameters processParameters\nWARNING: Parameters: " +
                "Invalid chunk ignored.",
            "Aug 29, 2009 12:03:40 AM org.apache.tomcat.util.http.Parameters processParameters\nWARNING: Parameters: " +
                "Invalid chunk ignored.",
            "Aug 29, 2009 12:03:45 AM org.apache.tomcat.util.http.Parameters processParameters\nWARNING: Parameters: " +
                "Invalid chunk ignored.",
            "Aug 29, 2009 12:03:57 AM org.apache.tomcat.util.http.Parameters processParameters\nWARNING: Parameters: " +
                "Invalid chunk ignored.");
        Map<String, String> mappings = new HashMap<>();

        assertEquals("%{CATALINA_DATESTAMP:_timestamp} .*",
            GrokPatternCreator.createGrokPatternFromExamples(sampleMessages, "CATALINA_DATESTAMP", "_timestamp", mappings));
        assertEquals(0, mappings.size());
    }

    public void testCreateGrokPatternFromExamplesGivenMultiTimestampLogs() {

        // Two timestamps: one local, one UTC
        Collection<String> sampleMessages = Arrays.asList(
            "559550912540598297\t2016-04-20T14:06:53\t2016-04-20T21:06:53Z\t38545844\tserv02nw07\t192.168.114.28\tAuthpriv\t" +
                "Info\tsshd\tsubsystem request for sftp",
            "559550912548986880\t2016-04-20T14:06:53\t2016-04-20T21:06:53Z\t9049724\tserv02nw03\t10.120.48.147\tAuthpriv\t" +
                "Info\tsshd\tsubsystem request for sftp",
            "559550912548986887\t2016-04-20T14:06:53\t2016-04-20T21:06:53Z\t884343\tserv02tw03\t192.168.121.189\tAuthpriv\t" +
                "Info\tsshd\tsubsystem request for sftp",
            "559550912603512850\t2016-04-20T14:06:53\t2016-04-20T21:06:53Z\t8907014\tserv02nw01\t192.168.118.208\tAuthpriv\t" +
                "Info\tsshd\tsubsystem request for sftp");
        Map<String, String> mappings = new HashMap<>();

        assertEquals("%{INT:field}\t%{TIMESTAMP_ISO8601:_timestamp}\t%{TIMESTAMP_ISO8601:extra_timestamp}\t%{INT:field2}\t.*?\t" +
                "%{IP:ipaddress}\t.*?\t%{LOGLEVEL:loglevel}\t.*",
            GrokPatternCreator.createGrokPatternFromExamples(sampleMessages, "TIMESTAMP_ISO8601", "_timestamp", mappings));
        assertEquals(5, mappings.size());
        assertEquals("long", mappings.get("field"));
        assertEquals("date", mappings.get("extra_timestamp"));
        assertEquals("long", mappings.get("field2"));
        assertEquals("ip", mappings.get("ipaddress"));
        assertEquals("keyword", mappings.get("loglevel"));
    }
}
