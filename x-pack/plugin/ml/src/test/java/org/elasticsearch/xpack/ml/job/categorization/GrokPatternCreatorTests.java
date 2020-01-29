/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.categorization;

import org.elasticsearch.grok.Grok;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class GrokPatternCreatorTests extends ESTestCase {

    public void testBuildFieldName() {
        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        assertEquals("field", GrokPatternCreator.buildFieldName(fieldNameCountStore, "field"));
        assertEquals("field2", GrokPatternCreator.buildFieldName(fieldNameCountStore, "field"));
        assertEquals("field3", GrokPatternCreator.buildFieldName(fieldNameCountStore, "field"));
        assertEquals("timestamp", GrokPatternCreator.buildFieldName(fieldNameCountStore, "timestamp"));
        assertEquals("field4", GrokPatternCreator.buildFieldName(fieldNameCountStore, "field"));
        assertEquals("uri", GrokPatternCreator.buildFieldName(fieldNameCountStore, "uri"));
        assertEquals("timestamp2", GrokPatternCreator.buildFieldName(fieldNameCountStore, "timestamp"));
        assertEquals("field5", GrokPatternCreator.buildFieldName(fieldNameCountStore, "field"));
    }

    public void testPopulatePrefacesAndEpiloguesGivenTimestamp() {

        Collection<String> matchingStrings = Arrays.asList("[2018-01-25T15:33:23] DEBUG ",
                "[2018-01-24T12:33:23] ERROR ",
                "junk [2018-01-22T07:33:23] INFO ",
                "[2018-01-21T03:33:23] DEBUG ");
        Grok grok = new GrokPatternCreator.GrokPatternCandidate("TIMESTAMP_ISO8601", "timestamp").grok;
        Collection<String> prefaces = new ArrayList<>();
        Collection<String> epilogues = new ArrayList<>();

        GrokPatternCreator.populatePrefacesAndEpilogues(matchingStrings, grok, prefaces, epilogues);

        assertThat(prefaces, containsInAnyOrder("[", "[", "junk [", "["));
        assertThat(epilogues, containsInAnyOrder("] DEBUG ", "] ERROR ", "] INFO ", "] DEBUG "));
    }

    public void testPopulatePrefacesAndEpiloguesGivenEmailAddress() {

        Collection<String> matchingStrings = Arrays.asList("before alice@acme.com after",
                "abc bob@acme.com xyz",
                "carol@acme.com");
        Grok grok = new GrokPatternCreator.GrokPatternCandidate("EMAILADDRESS", "email").grok;
        Collection<String> prefaces = new ArrayList<>();
        Collection<String> epilogues = new ArrayList<>();

        GrokPatternCreator.populatePrefacesAndEpilogues(matchingStrings, grok, prefaces, epilogues);

        assertThat(prefaces, containsInAnyOrder("before ", "abc ", ""));
        assertThat(epilogues, containsInAnyOrder(" after", " xyz", ""));
    }

    public void testAppendBestGrokMatchForStringsGivenTimestampsAndLogLevels() {

        Collection<String> mustMatchStrings = Arrays.asList("[2018-01-25T15:33:23] DEBUG ",
                "[2018-01-24T12:33:23] ERROR ",
                "junk [2018-01-22T07:33:23] INFO ",
                "[2018-01-21T03:33:23] DEBUG ");

        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        StringBuilder overallGrokPatternBuilder = new StringBuilder();

        GrokPatternCreator.appendBestGrokMatchForStrings("foo", fieldNameCountStore, overallGrokPatternBuilder, false,
            false, mustMatchStrings);

        assertEquals(".+?%{TIMESTAMP_ISO8601:timestamp}.+?%{LOGLEVEL:loglevel}.+?", overallGrokPatternBuilder.toString());
    }

    public void testAppendBestGrokMatchForStringsGivenTomcatDatestamps() {

        // The first part of the Tomcat datestamp can match as an ISO8601
        // timestamp if the ordering of candidate patterns is wrong
        Collection<String> mustMatchStrings = Arrays.asList("2018-09-03 17:03:28,269 +0100 | ERROR | ",
                "2018-09-03 17:04:27,279 +0100 | DEBUG | ",
                "2018-09-03 17:05:26,289 +0100 | ERROR | ");

        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        StringBuilder overallGrokPatternBuilder = new StringBuilder();

        GrokPatternCreator.appendBestGrokMatchForStrings("foo", fieldNameCountStore, overallGrokPatternBuilder, false,
            false, mustMatchStrings);

        assertEquals(".*?%{TOMCAT_DATESTAMP:timestamp}.+?%{LOGLEVEL:loglevel}.+?", overallGrokPatternBuilder.toString());
    }

    public void testAppendBestGrokMatchForStringsGivenTrappyFloatCandidates() {

        // If we're not careful then we might detect the first part of these strings as a
        // number, e.g. 1.2 in the first example, but this is inappropriate given the
        // trailing dot and digit
        Collection<String> mustMatchStrings = Arrays.asList("1.2.3",
                "-2.3.4",
                "4.5.6.7",
                "-9.8.7.6.5");

        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        StringBuilder overallGrokPatternBuilder = new StringBuilder();

        GrokPatternCreator.appendBestGrokMatchForStrings("foo", fieldNameCountStore, overallGrokPatternBuilder, false,
            false, mustMatchStrings);

        assertEquals(".+?", overallGrokPatternBuilder.toString());
    }

    public void testAppendBestGrokMatchForStringsGivenNumbersInBrackets() {

        Collection<String> mustMatchStrings = Arrays.asList("(-2)",
                "  (-3)",
                " (4)",
                " (-5) ");

        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        StringBuilder overallGrokPatternBuilder = new StringBuilder();

        GrokPatternCreator.appendBestGrokMatchForStrings("foo", fieldNameCountStore, overallGrokPatternBuilder, false,
            false, mustMatchStrings);

        assertEquals(".+?%{NUMBER:field}.+?", overallGrokPatternBuilder.toString());
    }

    public void testAppendBestGrokMatchForStringsGivenNegativeNumbersWithoutBreak() {

        Collection<String> mustMatchStrings = Arrays.asList("before-2 ",
                "prior to-3",
                "-4");

        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        StringBuilder overallGrokPatternBuilder = new StringBuilder();

        GrokPatternCreator.appendBestGrokMatchForStrings("foo", fieldNameCountStore, overallGrokPatternBuilder, false,
            false, mustMatchStrings);

        // It seems sensible that we don't detect these suffices as either base 10 or base 16 numbers
        assertEquals(".+?", overallGrokPatternBuilder.toString());
    }

    public void testAppendBestGrokMatchForStringsGivenHexNumbers() {

        Collection<String> mustMatchStrings = Arrays.asList(" abc",
                "  123",
                " -123",
                "1f is hex");

        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        StringBuilder overallGrokPatternBuilder = new StringBuilder();

        GrokPatternCreator.appendBestGrokMatchForStrings("foo", fieldNameCountStore, overallGrokPatternBuilder, false,
            false, mustMatchStrings);

        assertEquals(".*?%{BASE16NUM:field}.*?", overallGrokPatternBuilder.toString());
    }

    public void testAppendBestGrokMatchForStringsGivenHostnamesWithNumbers() {

        Collection<String> mustMatchStrings = Arrays.asList("<host1.1.p2ps:",
                "<host2.1.p2ps:");

        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        StringBuilder overallGrokPatternBuilder = new StringBuilder();

        GrokPatternCreator.appendBestGrokMatchForStrings("foo", fieldNameCountStore, overallGrokPatternBuilder, false,
            false, mustMatchStrings);

        // We don't want the .1. in the middle to get detected as a hex number
        assertEquals(".+?", overallGrokPatternBuilder.toString());
    }

    public void testAppendBestGrokMatchForStringsGivenEmailAddresses() {

        Collection<String> mustMatchStrings = Arrays.asList("before alice@acme.com after",
                "abc bob@acme.com xyz",
                "carol@acme.com");

        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        StringBuilder overallGrokPatternBuilder = new StringBuilder();

        GrokPatternCreator.appendBestGrokMatchForStrings("foo", fieldNameCountStore, overallGrokPatternBuilder, false,
            false, mustMatchStrings);

        assertEquals(".*?%{EMAILADDRESS:email}.*?", overallGrokPatternBuilder.toString());
    }

    public void testAppendBestGrokMatchForStringsGivenUris() {

        Collection<String> mustMatchStrings = Arrays.asList("main site https://www.elastic.co/ with trailing slash",
                "https://www.elastic.co/guide/en/x-pack/current/ml-configuring-categories.html#ml-configuring-categories is a section",
                "download today from https://www.elastic.co/downloads");

        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        StringBuilder overallGrokPatternBuilder = new StringBuilder();

        GrokPatternCreator.appendBestGrokMatchForStrings("foo", fieldNameCountStore, overallGrokPatternBuilder, false,
            false, mustMatchStrings);

        assertEquals(".*?%{URI:uri}.*?", overallGrokPatternBuilder.toString());
    }

    public void testAppendBestGrokMatchForStringsGivenPaths() {

        Collection<String> mustMatchStrings = Arrays.asList("on Mac /Users/dave",
                "on Windows C:\\Users\\dave",
                "on Linux /home/dave");

        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        StringBuilder overallGrokPatternBuilder = new StringBuilder();

        GrokPatternCreator.appendBestGrokMatchForStrings("foo", fieldNameCountStore, overallGrokPatternBuilder, false,
            false, mustMatchStrings);

        assertEquals(".+?%{PATH:path}.*?", overallGrokPatternBuilder.toString());
    }

    public void testFindBestGrokMatchFromExamplesGivenNamedLogs() {

        String regex = ".*?linux.+?named.+?error.+?unexpected.+?RCODE.+?REFUSED.+?resolving.*";
        Collection<String> examples = Arrays.asList(
                "Sep  8 11:55:06 linux named[22529]: error (unexpected RCODE REFUSED) resolving 'elastic.slack.com/A/IN': 95.110.64.205#53",
                "Sep  8 11:55:08 linux named[22529]: error (unexpected RCODE REFUSED) resolving 'slack-imgs.com/A/IN': 95.110.64.205#53",
                "Sep  8 11:55:35 linux named[22529]: error (unexpected RCODE REFUSED) resolving 'www.elastic.co/A/IN': 95.110.68.206#53",
                "Sep  8 11:55:42 linux named[22529]: error (unexpected RCODE REFUSED) resolving 'b.akamaiedge.net/A/IN': 95.110.64.205#53");

        assertEquals(".*?%{SYSLOGTIMESTAMP:timestamp}.+?linux.+?named.+?%{NUMBER:field}.+?error.+?" +
                "unexpected.+?RCODE.+?REFUSED.+?resolving.+?%{QUOTEDSTRING:field2}.+?%{IP:ipaddress}.+?%{NUMBER:field3}.*",
                GrokPatternCreator.findBestGrokMatchFromExamples("foo", regex, examples));
    }

    public void testFindBestGrokMatchFromExamplesGivenCatalinaLogs() {

        String regex = ".*?org\\.apache\\.tomcat\\.util\\.http\\.Parameters.+?processParameters.+?WARNING.+?Parameters.+?" +
                "Invalid.+?chunk.+?ignored.*";
        // The embedded newline ensures the regular expressions we're using are compiled with Pattern.DOTALL
        Collection<String> examples = Arrays.asList(
                "Aug 29, 2009 12:03:33 AM org.apache.tomcat.util.http.Parameters processParameters\nWARNING: Parameters: " +
                        "Invalid chunk ignored.",
                "Aug 29, 2009 12:03:40 AM org.apache.tomcat.util.http.Parameters processParameters\nWARNING: Parameters: " +
                        "Invalid chunk ignored.",
                "Aug 29, 2009 12:03:45 AM org.apache.tomcat.util.http.Parameters processParameters\nWARNING: Parameters: " +
                        "Invalid chunk ignored.",
                "Aug 29, 2009 12:03:57 AM org.apache.tomcat.util.http.Parameters processParameters\nWARNING: Parameters: " +
                        "Invalid chunk ignored.");

        assertEquals(".*?%{CATALINA_DATESTAMP:timestamp}.+?org\\.apache\\.tomcat\\.util\\.http\\.Parameters.+?processParameters.+?" +
                "WARNING.+?Parameters.+?Invalid.+?chunk.+?ignored.*",
                GrokPatternCreator.findBestGrokMatchFromExamples("foo", regex, examples));
    }

    public void testFindBestGrokMatchFromExamplesGivenMultiTimestampLogs() {

        String regex = ".*?Authpriv.+?Info.+?sshd.+?subsystem.+?request.+?for.+?sftp.*";
        // Two timestamps: one local, one UTC
        Collection<String> examples = Arrays.asList(
                "559550912540598297\t2016-04-20T14:06:53\t2016-04-20T21:06:53Z\t38545844\tserv02nw07\t192.168.114.28\tAuthpriv\t" +
                        "Info\tsshd\tsubsystem request for sftp",
                "559550912548986880\t2016-04-20T14:06:53\t2016-04-20T21:06:53Z\t9049724\tserv02nw03\t10.120.48.147\tAuthpriv\t" +
                        "Info\tsshd\tsubsystem request for sftp",
                "559550912548986887\t2016-04-20T14:06:53\t2016-04-20T21:06:53Z\t884343\tserv02tw03\t192.168.121.189\tAuthpriv\t" +
                        "Info\tsshd\tsubsystem request for sftp",
                "559550912603512850\t2016-04-20T14:06:53\t2016-04-20T21:06:53Z\t8907014\tserv02nw01\t192.168.118.208\tAuthpriv\t" +
                        "Info\tsshd\tsubsystem request for sftp");

        assertEquals(".*?%{NUMBER:field}.+?%{TIMESTAMP_ISO8601:timestamp}.+?%{TIMESTAMP_ISO8601:timestamp2}.+?%{NUMBER:field2}.+?" +
                "%{IP:ipaddress}.+?Authpriv.+?Info.+?sshd.+?subsystem.+?request.+?for.+?sftp.*",
                GrokPatternCreator.findBestGrokMatchFromExamples("foo", regex, examples));
    }

    public void testFindBestGrokMatchFromExamplesGivenAdversarialInputRecurseDepth() {
        String regex = ".*?combo.+?rpc\\.statd.+?gethostbyname.+?error.+?for.+?X.+?X.+?Z.+?Z.+?hn.+?hn.*";
        // Two timestamps: one local, one UTC
        Collection<String> examples = Arrays.asList(
            "combo rpc.statd[1605]: gethostbyname error for ^X^X^Z^Z%8x%8x%8x%8x%8x%8x%8x%8x%8x%62716x%hn%51859x%hn" +
                "\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\22...",
            "combo rpc.statd[1608]: gethostbyname error for ^X^X^Z^Z%8x%8x%8x%8x%8x%8x%8x%8x%8x%62716x%hn%51859x%hn" +
                "\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\22...",
            "combo rpc.statd[1635]: gethostbyname error for ^X^X^Z^Z%8x%8x%8x%8x%8x%8x%8x%8x%8x%62716x%hn%51859x%hn" +
                "\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220\\220" +
                "\\220\\220\\220\\220\\220\\220\\220\\220\\220\\22...");
        assertEquals(
            ".*?combo.+?rpc\\.statd.+?%{NUMBER:field}.+?gethostbyname.+?error.+?for.+?X.+?X.+?Z.+?Z.+?hn.+?hn.+?%{NUMBER:field2}" +
                ".+?%{NUMBER:field3}.+?%{NUMBER:field4}.+?%{NUMBER:field5}.+?%{NUMBER:field6}.+?%{NUMBER:field7}.+?%{NUMBER:field8}" +
                ".+?%{NUMBER:field9}.+?%{NUMBER:field10}.+?%{NUMBER:field11}.*",
            GrokPatternCreator.findBestGrokMatchFromExamples("foo", regex, examples));
    }

    public void testFindBestGrokMatchFromExamplesGivenMatchAllRegex() {
        String regex = ".*";
        // Two timestamps: one local, one UTC
        Collection<String> examples = Arrays.asList(
            "Killing job [count_tweets]",
            "Killing job [tweets_by_location]",
            "[count_tweets] Killing job",
            "[tweets_by_location] Killing job");
        assertThat(GrokPatternCreator.findBestGrokMatchFromExamples("foo", regex, examples), equalTo(regex));
    }
}
