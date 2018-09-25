/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.filestructurefinder;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.xpack.ml.filestructurefinder.TimestampFormatFinder.TimestampMatch;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class TimestampFormatFinderTests extends FileStructureTestCase {

    public void testFindFirstMatchGivenNoMatch() {

        assertNull(TimestampFormatFinder.findFirstMatch(""));
        assertNull(TimestampFormatFinder.findFirstMatch("no timestamps in here"));
        assertNull(TimestampFormatFinder.findFirstMatch(":::"));
        assertNull(TimestampFormatFinder.findFirstMatch("/+"));
    }

    public void testFindFirstMatchGivenOnlyIso8601() {

        validateTimestampMatch(new TimestampMatch(7, "", "ISO8601", "yyyy-MM-dd'T'HH:mm:ss,SSSXX",
                "\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2},\\d{3}", "TIMESTAMP_ISO8601", ""), "2018-05-15T16:14:56,374Z",
            1526400896374L);
        validateTimestampMatch(new TimestampMatch(7, "", "ISO8601", "yyyy-MM-dd'T'HH:mm:ss,SSSXX",
                "\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2},\\d{3}", "TIMESTAMP_ISO8601", ""), "2018-05-15T17:14:56,374+0100",
            1526400896374L);
        validateTimestampMatch(new TimestampMatch(8, "", "ISO8601", "yyyy-MM-dd'T'HH:mm:ss,SSSXXX",
                "\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2},\\d{3}", "TIMESTAMP_ISO8601", ""), "2018-05-15T17:14:56,374+01:00",
            1526400896374L);
        validateTimestampMatch(new TimestampMatch(9, "", "ISO8601", "yyyy-MM-dd'T'HH:mm:ss,SSS",
            "\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2},\\d{3}", "TIMESTAMP_ISO8601", ""), "2018-05-15T17:14:56,374", 1526400896374L);

        TimestampMatch pureIso8601Expected = new TimestampMatch(10, "", "ISO8601", "ISO8601",
            "\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}", "TIMESTAMP_ISO8601", "");

        validateTimestampMatch(pureIso8601Expected, "2018-05-15T16:14:56Z", 1526400896000L);
        validateTimestampMatch(pureIso8601Expected, "2018-05-15T17:14:56+0100", 1526400896000L);
        validateTimestampMatch(pureIso8601Expected, "2018-05-15T17:14:56+01:00", 1526400896000L);
        validateTimestampMatch(pureIso8601Expected, "2018-05-15T17:14:56", 1526400896000L);

        validateTimestampMatch(new TimestampMatch(1, "", "YYYY-MM-dd HH:mm:ss,SSSZ", "yyyy-MM-dd HH:mm:ss,SSSXX",
                "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}", "TIMESTAMP_ISO8601", ""), "2018-05-15 16:14:56,374Z",
            1526400896374L);
        validateTimestampMatch(new TimestampMatch(1, "", "YYYY-MM-dd HH:mm:ss,SSSZ", "yyyy-MM-dd HH:mm:ss,SSSXX",
                "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}", "TIMESTAMP_ISO8601", ""), "2018-05-15 17:14:56,374+0100",
            1526400896374L);
        validateTimestampMatch(new TimestampMatch(2, "", "YYYY-MM-dd HH:mm:ss,SSSZZ", "yyyy-MM-dd HH:mm:ss,SSSXXX",
                "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}", "TIMESTAMP_ISO8601", ""), "2018-05-15 17:14:56,374+01:00",
            1526400896374L);
        validateTimestampMatch(new TimestampMatch(3, "", "YYYY-MM-dd HH:mm:ss,SSS", "yyyy-MM-dd HH:mm:ss,SSS",
                "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}", "TIMESTAMP_ISO8601", ""), "2018-05-15 17:14:56,374", 1526400896374L);
        validateTimestampMatch(new TimestampMatch(4, "", "YYYY-MM-dd HH:mm:ssZ", "yyyy-MM-dd HH:mm:ssXX",
                "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}", "TIMESTAMP_ISO8601", ""), "2018-05-15 16:14:56Z", 1526400896000L);
        validateTimestampMatch(new TimestampMatch(4, "", "YYYY-MM-dd HH:mm:ssZ", "yyyy-MM-dd HH:mm:ssXX",
                "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}", "TIMESTAMP_ISO8601", ""), "2018-05-15 17:14:56+0100", 1526400896000L);
        validateTimestampMatch(new TimestampMatch(5, "", "YYYY-MM-dd HH:mm:ssZZ", "yyyy-MM-dd HH:mm:ssXXX",
                "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}", "TIMESTAMP_ISO8601", ""), "2018-05-15 17:14:56+01:00", 1526400896000L);
        validateTimestampMatch(new TimestampMatch(6, "", "YYYY-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss",
                "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}", "TIMESTAMP_ISO8601", ""), "2018-05-15 17:14:56", 1526400896000L);
    }

    public void testFindFirstMatchGivenOnlyKnownTimestampFormat() {

        // Note: some of the time formats give millisecond accuracy, some second accuracy and some minute accuracy

        validateTimestampMatch(new TimestampMatch(0, "", "YYYY-MM-dd HH:mm:ss,SSS Z", "yyyy-MM-dd HH:mm:ss,SSS XX",
                "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}", "TOMCAT_DATESTAMP", ""), "2018-05-15 17:14:56,374 +0100",
            1526400896374L);

        validateTimestampMatch(new TimestampMatch(11, "", "EEE MMM dd YYYY HH:mm:ss zzz", "EEE MMM dd yyyy HH:mm:ss zzz",
                "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{4} \\d{2}:\\d{2}:\\d{2} ", "DATESTAMP_RFC822", ""),
            "Tue May 15 2018 16:14:56 UTC", 1526400896000L);
        validateTimestampMatch(new TimestampMatch(12, "", "EEE MMM dd YYYY HH:mm zzz", "EEE MMM dd yyyy HH:mm zzz",
                "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{4} \\d{2}:\\d{2} ", "DATESTAMP_RFC822", ""),
            "Tue May 15 2018 16:14 UTC", 1526400840000L);

        validateTimestampMatch(new TimestampMatch(13, "", "EEE, dd MMM YYYY HH:mm:ss ZZ", "EEE, dd MMM yyyy HH:mm:ss XXX",
                "\\b[A-Z]\\S{2,8}, \\d{1,2} [A-Z]\\S{2,8} \\d{4} \\d{2}:\\d{2}:\\d{2} ", "DATESTAMP_RFC2822", ""),
            "Tue, 15 May 2018 17:14:56 +01:00", 1526400896000L);
        validateTimestampMatch(new TimestampMatch(14, "", "EEE, dd MMM YYYY HH:mm:ss Z", "EEE, dd MMM yyyy HH:mm:ss XX",
                "\\b[A-Z]\\S{2,8}, \\d{1,2} [A-Z]\\S{2,8} \\d{4} \\d{2}:\\d{2}:\\d{2} ", "DATESTAMP_RFC2822", ""),
            "Tue, 15 May 2018 17:14:56 +0100", 1526400896000L);
        validateTimestampMatch(new TimestampMatch(15, "", "EEE, dd MMM YYYY HH:mm ZZ", "EEE, dd MMM yyyy HH:mm XXX",
                "\\b[A-Z]\\S{2,8}, \\d{1,2} [A-Z]\\S{2,8} \\d{4} \\d{2}:\\d{2} ", "DATESTAMP_RFC2822", ""),
            "Tue, 15 May 2018 17:14 +01:00", 1526400840000L);
        validateTimestampMatch(new TimestampMatch(16, "", "EEE, dd MMM YYYY HH:mm Z", "EEE, dd MMM yyyy HH:mm XX",
                "\\b[A-Z]\\S{2,8}, \\d{1,2} [A-Z]\\S{2,8} \\d{4} \\d{2}:\\d{2} ", "DATESTAMP_RFC2822", ""), "Tue, 15 May 2018 17:14 +0100",
            1526400840000L);

        validateTimestampMatch(new TimestampMatch(17, "", "EEE MMM dd HH:mm:ss zzz YYYY", "EEE MMM dd HH:mm:ss zzz yyyy",
                "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{2}:\\d{2}:\\d{2} [A-Z]{3,4} \\d{4}\\b", "DATESTAMP_OTHER", ""),
            "Tue May 15 16:14:56 UTC 2018", 1526400896000L);
        validateTimestampMatch(new TimestampMatch(18, "", "EEE MMM dd HH:mm zzz YYYY", "EEE MMM dd HH:mm zzz yyyy",
                "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{2}:\\d{2} [A-Z]{3,4} \\d{4}\\b", "DATESTAMP_OTHER", ""),
            "Tue May 15 16:14 UTC 2018", 1526400840000L);

        validateTimestampMatch(new TimestampMatch(19, "", "YYYYMMddHHmmss", "yyyyMMddHHmmss", "\\b\\d{14}\\b",
                "DATESTAMP_EVENTLOG", ""),
            "20180515171456", 1526400896000L);

        validateTimestampMatch(new TimestampMatch(20, "", "EEE MMM dd HH:mm:ss YYYY", "EEE MMM dd HH:mm:ss yyyy",
                "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{2}:\\d{2}:\\d{2} \\d{4}\\b", "HTTPDERROR_DATE", ""),
            "Tue May 15 17:14:56 2018", 1526400896000L);

        validateTimestampMatch(new TimestampMatch(21, "", Arrays.asList("MMM dd HH:mm:ss.SSS", "MMM  d HH:mm:ss.SSS"),
            Arrays.asList("MMM dd HH:mm:ss.SSS", "MMM  d HH:mm:ss.SSS"),
            "\\b[A-Z]\\S{2,8} {1,2}\\d{1,2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}", "SYSLOGTIMESTAMP", ""), "May 15 17:14:56.725", 1526400896725L);
        validateTimestampMatch(new TimestampMatch(22, "", Arrays.asList("MMM dd HH:mm:ss", "MMM  d HH:mm:ss"),
            Arrays.asList("MMM dd HH:mm:ss", "MMM  d HH:mm:ss"),
            "\\b[A-Z]\\S{2,8} {1,2}\\d{1,2} \\d{2}:\\d{2}:\\d{2}\\b", "SYSLOGTIMESTAMP", ""), "May 15 17:14:56", 1526400896000L);

        validateTimestampMatch(new TimestampMatch(23, "", "dd/MMM/YYYY:HH:mm:ss Z", "dd/MMM/yyyy:HH:mm:ss XX",
                "\\b\\d{2}/[A-Z]\\S{2}/\\d{4}:\\d{2}:\\d{2}:\\d{2} ", "HTTPDATE", ""), "15/May/2018:17:14:56 +0100", 1526400896000L);

        validateTimestampMatch(new TimestampMatch(24, "", "MMM dd, YYYY h:mm:ss a", "MMM dd, yyyy h:mm:ss a",
                "\\b[A-Z]\\S{2,8} \\d{1,2}, \\d{4} \\d{1,2}:\\d{2}:\\d{2} [AP]M\\b", "CATALINA_DATESTAMP", ""), "May 15, 2018 5:14:56 PM",
            1526400896000L);

        validateTimestampMatch(new TimestampMatch(25, "", Arrays.asList("MMM dd YYYY HH:mm:ss", "MMM  d YYYY HH:mm:ss"),
                Arrays.asList("MMM dd yyyy HH:mm:ss", "MMM  d yyyy HH:mm:ss"),
                "\\b[A-Z]\\S{2,8} {1,2}\\d{1,2} \\d{4} \\d{2}:\\d{2}:\\d{2}\\b", "CISCOTIMESTAMP", ""), "May 15 2018 17:14:56",
            1526400896000L);
    }

    public void testFindFirstMatchGivenOnlySystemDate() {

        assertEquals(new TimestampMatch(26, "", "UNIX_MS", "UNIX_MS", "\\b\\d{13}\\b", "POSINT", ""),
            TimestampFormatFinder.findFirstMatch("1526400896374"));
        assertEquals(new TimestampMatch(26, "", "UNIX_MS", "UNIX_MS", "\\b\\d{13}\\b", "POSINT", ""),
            TimestampFormatFinder.findFirstFullMatch("1526400896374"));

        assertEquals(new TimestampMatch(27, "", "UNIX", "UNIX", "\\b\\d{10}\\.\\d{3,9}\\b", "NUMBER", ""),
            TimestampFormatFinder.findFirstMatch("1526400896.736"));
        assertEquals(new TimestampMatch(27, "", "UNIX", "UNIX", "\\b\\d{10}\\.\\d{3,9}\\b", "NUMBER", ""),
            TimestampFormatFinder.findFirstFullMatch("1526400896.736"));
        assertEquals(new TimestampMatch(28, "", "UNIX", "UNIX", "\\b\\d{10}\\b", "POSINT", ""),
            TimestampFormatFinder.findFirstMatch("1526400896"));
        assertEquals(new TimestampMatch(28, "", "UNIX", "UNIX", "\\b\\d{10}\\b", "POSINT", ""),
            TimestampFormatFinder.findFirstFullMatch("1526400896"));

        assertEquals(new TimestampMatch(29, "", "TAI64N", "TAI64N", "\\b[0-9A-Fa-f]{24}\\b", "BASE16NUM", ""),
            TimestampFormatFinder.findFirstMatch("400000005afb159a164ac980"));
        assertEquals(new TimestampMatch(29, "", "TAI64N", "TAI64N", "\\b[0-9A-Fa-f]{24}\\b", "BASE16NUM", ""),
            TimestampFormatFinder.findFirstFullMatch("400000005afb159a164ac980"));
    }

    public void testFindFirstMatchGivenRealLogMessages() {

        assertEquals(new TimestampMatch(9, "[", "ISO8601", "yyyy-MM-dd'T'HH:mm:ss,SSS",
                "\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2},\\d{3}", "TIMESTAMP_ISO8601",
                "][INFO ][o.e.e.NodeEnvironment    ] [node-0] heap size [3.9gb], compressed ordinary object pointers [true]"),
            TimestampFormatFinder.findFirstMatch("[2018-05-11T17:07:29,553][INFO ][o.e.e.NodeEnvironment    ] [node-0] " +
                "heap size [3.9gb], compressed ordinary object pointers [true]"));

        assertEquals(new TimestampMatch(23, "192.168.62.101 - - [", "dd/MMM/YYYY:HH:mm:ss Z", "dd/MMM/yyyy:HH:mm:ss XX",
                "\\b\\d{2}/[A-Z]\\S{2}/\\d{4}:\\d{2}:\\d{2}:\\d{2} ", "HTTPDATE",
                "] \"POST //apiserv:8080/engine/v2/jobs HTTP/1.1\" 201 42 \"-\" \"curl/7.46.0\" 384"),
            TimestampFormatFinder.findFirstMatch("192.168.62.101 - - [29/Jun/2016:12:11:31 +0000] " +
                "\"POST //apiserv:8080/engine/v2/jobs HTTP/1.1\" 201 42 \"-\" \"curl/7.46.0\" 384"));

        assertEquals(new TimestampMatch(24, "", "MMM dd, YYYY h:mm:ss a", "MMM dd, yyyy h:mm:ss a",
                "\\b[A-Z]\\S{2,8} \\d{1,2}, \\d{4} \\d{1,2}:\\d{2}:\\d{2} [AP]M\\b", "CATALINA_DATESTAMP",
                " org.apache.tomcat.util.http.Parameters processParameters"),
            TimestampFormatFinder.findFirstMatch("Aug 29, 2009 12:03:57 AM org.apache.tomcat.util.http.Parameters processParameters"));

        assertEquals(new TimestampMatch(22, "", Arrays.asList("MMM dd HH:mm:ss", "MMM  d HH:mm:ss"),
                Arrays.asList("MMM dd HH:mm:ss", "MMM  d HH:mm:ss"),
                "\\b[A-Z]\\S{2,8} {1,2}\\d{1,2} \\d{2}:\\d{2}:\\d{2}\\b", "SYSLOGTIMESTAMP", " esxi1.acme.com Vpxa: " +
                    "[3CB3FB90 verbose 'vpxavpxaInvtVm' opID=WFU-33d82c31] [VpxaInvtVmChangeListener] Guest DiskInfo Changed"),
            TimestampFormatFinder.findFirstMatch("Oct 19 17:04:44 esxi1.acme.com Vpxa: [3CB3FB90 verbose 'vpxavpxaInvtVm' " +
                "opID=WFU-33d82c31] [VpxaInvtVmChangeListener] Guest DiskInfo Changed"));

        assertEquals(new TimestampMatch(10, "559550912540598297\t", "ISO8601", "ISO8601", "\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}",
                "TIMESTAMP_ISO8601",
                "\t2016-04-20T21:06:53Z\t38545844\tserv02nw07\t192.168.114.28\tAuthpriv\tInfo\tsshd\tsubsystem request for sftp"),
            TimestampFormatFinder.findFirstMatch("559550912540598297\t2016-04-20T14:06:53\t2016-04-20T21:06:53Z\t38545844\tserv02nw07\t" +
                "192.168.114.28\tAuthpriv\tInfo\tsshd\tsubsystem request for sftp"));

        assertEquals(new TimestampMatch(22, "", Arrays.asList("MMM dd HH:mm:ss", "MMM  d HH:mm:ss"),
                Arrays.asList("MMM dd HH:mm:ss", "MMM  d HH:mm:ss"),
                "\\b[A-Z]\\S{2,8} {1,2}\\d{1,2} \\d{2}:\\d{2}:\\d{2}\\b", "SYSLOGTIMESTAMP",
                " dnsserv named[22529]: error (unexpected RCODE REFUSED) resolving 'www.elastic.co/A/IN': 95.110.68.206#53"),
            TimestampFormatFinder.findFirstMatch("Sep  8 11:55:35 dnsserv named[22529]: error (unexpected RCODE REFUSED) resolving " +
                "'www.elastic.co/A/IN': 95.110.68.206#53"));

        assertEquals(new TimestampMatch(3, "", "YYYY-MM-dd HH:mm:ss.SSSSSS", "yyyy-MM-dd HH:mm:ss.SSSSSS",
                "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}", "TIMESTAMP_ISO8601",
                "|INFO    |VirtualServer |1  |client  'User1'(id:2) was added to channelgroup 'Channel Admin'(id:5) by client " +
                    "'User1'(id:2) in channel '3er Instanz'(id:2)"),
            TimestampFormatFinder.findFirstMatch("2018-01-06 19:22:20.106822|INFO    |VirtualServer |1  |client " +
                " 'User1'(id:2) was added to channelgroup 'Channel Admin'(id:5) by client 'User1'(id:2) in channel '3er Instanz'(id:2)"));

        // Differs from the above as the required format is specified
        assertEquals(new TimestampMatch(3, "", "YYYY-MM-dd HH:mm:ss.SSSSSS", "yyyy-MM-dd HH:mm:ss.SSSSSS",
                "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}", "TIMESTAMP_ISO8601",
                "|INFO    |VirtualServer |1  |client  'User1'(id:2) was added to channelgroup 'Channel Admin'(id:5) by client " +
                    "'User1'(id:2) in channel '3er Instanz'(id:2)"),
            TimestampFormatFinder.findFirstMatch("2018-01-06 19:22:20.106822|INFO    |VirtualServer |1  |client " +
                " 'User1'(id:2) was added to channelgroup 'Channel Admin'(id:5) by client 'User1'(id:2) in channel '3er Instanz'(id:2)",
                randomFrom("YYYY-MM-dd HH:mm:ss.SSSSSS", "yyyy-MM-dd HH:mm:ss.SSSSSS")));

        // Non-matching required format specified
        assertNull(TimestampFormatFinder.findFirstMatch("2018-01-06 19:22:20.106822|INFO    |VirtualServer |1  |client " +
                " 'User1'(id:2) was added to channelgroup 'Channel Admin'(id:5) by client 'User1'(id:2) in channel '3er Instanz'(id:2)",
            randomFrom("UNIX", "EEE MMM dd YYYY HH:mm zzz")));
    }

    public void testAdjustRequiredFormat() {
        assertEquals("YYYY-MM-dd HH:mm:ss,SSS Z", TimestampFormatFinder.adjustRequiredFormat("YYYY-MM-dd HH:mm:ss,SSS Z"));
        assertEquals("YYYY-MM-dd HH:mm:ss,SSS Z", TimestampFormatFinder.adjustRequiredFormat("YYYY-MM-dd HH:mm:ss,SSSSSS Z"));
        assertEquals("YYYY-MM-dd HH:mm:ss,SSS Z", TimestampFormatFinder.adjustRequiredFormat("YYYY-MM-dd HH:mm:ss,SSSSSSSSS Z"));
        assertEquals("YYYY-MM-dd HH:mm:ss,SSS Z", TimestampFormatFinder.adjustRequiredFormat("YYYY-MM-dd HH:mm:ss.SSS Z"));
        assertEquals("YYYY-MM-dd HH:mm:ss,SSS Z", TimestampFormatFinder.adjustRequiredFormat("YYYY-MM-dd HH:mm:ss.SSSSSS Z"));
        assertEquals("YYYY-MM-dd HH:mm:ss,SSS Z", TimestampFormatFinder.adjustRequiredFormat("YYYY-MM-dd HH:mm:ss.SSSSSSSSS Z"));
        assertEquals("YYYY-MM-dd HH:mm:ss,SSS", TimestampFormatFinder.adjustRequiredFormat("YYYY-MM-dd HH:mm:ss,SSS"));
        assertEquals("YYYY-MM-dd HH:mm:ss,SSS", TimestampFormatFinder.adjustRequiredFormat("YYYY-MM-dd HH:mm:ss,SSSSSS"));
        assertEquals("YYYY-MM-dd HH:mm:ss,SSS", TimestampFormatFinder.adjustRequiredFormat("YYYY-MM-dd HH:mm:ss,SSSSSSSSS"));
        assertEquals("YYYY-MM-dd HH:mm:ss,SSS", TimestampFormatFinder.adjustRequiredFormat("YYYY-MM-dd HH:mm:ss.SSS"));
        assertEquals("YYYY-MM-dd HH:mm:ss,SSS", TimestampFormatFinder.adjustRequiredFormat("YYYY-MM-dd HH:mm:ss.SSSSSS"));
        assertEquals("YYYY-MM-dd HH:mm:ss,SSS", TimestampFormatFinder.adjustRequiredFormat("YYYY-MM-dd HH:mm:ss.SSSSSSSSS"));
    }

    public void testInterpretFractionalSeconds() {
        assertEquals(new Tuple<>(',', 0), TimestampFormatFinder.interpretFractionalSeconds("Sep  8 11:55:35"));
        assertEquals(new Tuple<>(',', 0), TimestampFormatFinder.interpretFractionalSeconds("29/Jun/2016:12:11:31 +0000"));
        assertEquals(new Tuple<>('.', 6), TimestampFormatFinder.interpretFractionalSeconds("2018-01-06 17:21:25.764368"));
        assertEquals(new Tuple<>(',', 9), TimestampFormatFinder.interpretFractionalSeconds("2018-01-06T17:21:25,764363438"));
        assertEquals(new Tuple<>(',', 3), TimestampFormatFinder.interpretFractionalSeconds("2018-01-06T17:21:25,764"));
        assertEquals(new Tuple<>('.', 3), TimestampFormatFinder.interpretFractionalSeconds("2018-01-06T17:21:25.764"));
        assertEquals(new Tuple<>('.', 6), TimestampFormatFinder.interpretFractionalSeconds("2018-01-06 17:21:25.764368Z"));
        assertEquals(new Tuple<>(',', 9), TimestampFormatFinder.interpretFractionalSeconds("2018-01-06T17:21:25,764363438Z"));
        assertEquals(new Tuple<>(',', 3), TimestampFormatFinder.interpretFractionalSeconds("2018-01-06T17:21:25,764Z"));
        assertEquals(new Tuple<>('.', 3), TimestampFormatFinder.interpretFractionalSeconds("2018-01-06T17:21:25.764Z"));
        assertEquals(new Tuple<>('.', 6), TimestampFormatFinder.interpretFractionalSeconds("2018-01-06 17:21:25.764368 Z"));
        assertEquals(new Tuple<>(',', 9), TimestampFormatFinder.interpretFractionalSeconds("2018-01-06T17:21:25,764363438 Z"));
        assertEquals(new Tuple<>(',', 3), TimestampFormatFinder.interpretFractionalSeconds("2018-01-06T17:21:25,764 Z"));
        assertEquals(new Tuple<>('.', 3), TimestampFormatFinder.interpretFractionalSeconds("2018-01-06T17:21:25.764 Z"));
    }

    private void validateTimestampMatch(TimestampMatch expected, String text, long expectedEpochMs) {

        assertEquals(expected, TimestampFormatFinder.findFirstMatch(text));
        assertEquals(expected, TimestampFormatFinder.findFirstFullMatch(text));
        assertEquals(expected, TimestampFormatFinder.findFirstMatch(text, expected.candidateIndex));
        assertEquals(expected, TimestampFormatFinder.findFirstFullMatch(text, expected.candidateIndex));
        assertNull(TimestampFormatFinder.findFirstMatch(text, Integer.MAX_VALUE));
        assertNull(TimestampFormatFinder.findFirstFullMatch(text, Integer.MAX_VALUE));
        assertEquals(expected, TimestampFormatFinder.findFirstMatch(text, randomFrom(expected.jodaTimestampFormats)));
        assertEquals(expected, TimestampFormatFinder.findFirstFullMatch(text, randomFrom(expected.jodaTimestampFormats)));
        assertEquals(expected, TimestampFormatFinder.findFirstMatch(text, randomFrom(expected.javaTimestampFormats)));
        assertEquals(expected, TimestampFormatFinder.findFirstFullMatch(text, randomFrom(expected.javaTimestampFormats)));
        assertNull(TimestampFormatFinder.findFirstMatch(text, "wrong format"));
        assertNull(TimestampFormatFinder.findFirstFullMatch(text, "wrong format"));

        validateJodaTimestampFormats(expected.jodaTimestampFormats, text, expectedEpochMs);
        validateJavaTimestampFormats(expected.javaTimestampFormats, text, expectedEpochMs);

        assertTrue(expected.simplePattern.matcher(text).find());
    }

    private void validateJodaTimestampFormats(List<String> jodaTimestampFormats, String text, long expectedEpochMs) {

        // All the test times are for Tue May 15 2018 16:14:56 UTC, which is 17:14:56 in London.
        // This is the timezone that will be used for any text representations that don't include it.
        org.joda.time.DateTimeZone defaultZone = org.joda.time.DateTimeZone.forID("Europe/London");
        org.joda.time.DateTime parsed;
        for (int i = 0; i < jodaTimestampFormats.size(); ++i) {
            try {
                String timestampFormat = jodaTimestampFormats.get(i);
                switch (timestampFormat) {
                    case "ISO8601":
                        parsed = org.joda.time.format.ISODateTimeFormat.dateTimeParser()
                            .withZone(defaultZone).withDefaultYear(2018).parseDateTime(text);
                        break;
                    default:
                        org.joda.time.format.DateTimeFormatter parser =
                            org.joda.time.format.DateTimeFormat.forPattern(timestampFormat).withZone(defaultZone).withLocale(Locale.ROOT);
                        parsed = parser.withDefaultYear(2018).parseDateTime(text);
                        break;
                }
                if (expectedEpochMs == parsed.getMillis()) {
                    break;
                }
                // If the last one isn't right then propagate
                if (i == jodaTimestampFormats.size() - 1) {
                    assertEquals(expectedEpochMs, parsed.getMillis());
                }
            } catch (RuntimeException e) {
                // If the last one throws then propagate
                if (i == jodaTimestampFormats.size() - 1) {
                    throw e;
                }
            }
        }
    }

    private void validateJavaTimestampFormats(List<String> javaTimestampFormats, String text, long expectedEpochMs) {

        // All the test times are for Tue May 15 2018 16:14:56 UTC, which is 17:14:56 in London.
        // This is the timezone that will be used for any text representations that don't include it.
        java.time.ZoneId defaultZone = java.time.ZoneId.of("Europe/London");
        java.time.temporal.TemporalAccessor parsed;
        for (int i = 0; i < javaTimestampFormats.size(); ++i) {
            try {
                String timestampFormat = javaTimestampFormats.get(i);
                switch (timestampFormat) {
                    case "ISO8601":
                        parsed = DateFormatters.forPattern("strict_date_optional_time_nanos").withZone(defaultZone).parse(text);
                        break;
                    default:
                        java.time.format.DateTimeFormatter parser = new java.time.format.DateTimeFormatterBuilder()
                            .appendPattern(timestampFormat).parseDefaulting(java.time.temporal.ChronoField.YEAR_OF_ERA, 2018)
                            .toFormatter(Locale.ROOT);
                        // This next line parses the textual date without any default timezone, so if
                        // the text doesn't contain the timezone then the resulting temporal accessor
                        // will be incomplete (i.e. impossible to convert to an Instant).  You would
                        // hope that it would be possible to specify a timezone to be used only in this
                        // case, and in Java 9 and 10 it is, by adding withZone(zone) before the
                        // parse(text) call.  However, with Java 8 this overrides any timezone parsed
                        // from the text.  The solution is to parse twice, once without a default
                        // timezone and then again with a default timezone if the first parse didn't
                        // find one in the text.
                        parsed = parser.parse(text);
                        if (parsed.query(java.time.temporal.TemporalQueries.zone()) == null) {
                            // TODO: when Java 8 is no longer supported remove the two
                            // lines and comment above and the closing brace below
                            parsed = parser.withZone(defaultZone).parse(text);
                        }
                        break;
                }
                long actualEpochMs = java.time.Instant.from(parsed).toEpochMilli();
                if (expectedEpochMs == actualEpochMs) {
                    break;
                }
                // If the last one isn't right then propagate
                if (i == javaTimestampFormats.size() - 1) {
                    assertEquals(expectedEpochMs, actualEpochMs);
                }
            } catch (RuntimeException e) {
                // If the last one throws then propagate
                if (i == javaTimestampFormats.size() - 1) {
                    throw e;
                }
            }
        }
    }
}
