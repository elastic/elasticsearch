/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.filestructurefinder;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.ml.filestructurefinder.TimestampFormatFinder.TimestampMatch;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Arrays;
import java.util.Locale;

public class TimestampFormatFinderTests extends FileStructureTestCase {

    public void testFindFirstMatchGivenNoMatch() {

        assertNull(TimestampFormatFinder.findFirstMatch(""));
        assertNull(TimestampFormatFinder.findFirstMatch("no timestamps in here"));
        assertNull(TimestampFormatFinder.findFirstMatch(":::"));
        assertNull(TimestampFormatFinder.findFirstMatch("/+"));
    }

    public void testFindFirstMatchGivenOnlyIso8601() {

        TimestampMatch expected = new TimestampMatch(7, "", "ISO8601", "\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}", "TIMESTAMP_ISO8601",
            "");

        checkAndValidateDateFormat(expected, "2018-05-15T16:14:56,374Z", 1526400896374L);
        checkAndValidateDateFormat(expected, "2018-05-15T17:14:56,374+0100", 1526400896374L);
        checkAndValidateDateFormat(expected, "2018-05-15T17:14:56,374+01:00", 1526400896374L);
        checkAndValidateDateFormat(expected, "2018-05-15T17:14:56,374", 1526400896374L);
        checkAndValidateDateFormat(expected, "2018-05-15T16:14:56Z", 1526400896000L);
        checkAndValidateDateFormat(expected, "2018-05-15T17:14:56+0100", 1526400896000L);
        checkAndValidateDateFormat(expected, "2018-05-15T17:14:56+01:00", 1526400896000L);
        checkAndValidateDateFormat(expected, "2018-05-15T17:14:56", 1526400896000L);

        checkAndValidateDateFormat(new TimestampMatch(1, "", "YYYY-MM-dd HH:mm:ss,SSSZ",
                "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}", "TIMESTAMP_ISO8601", ""), "2018-05-15 16:14:56,374Z",
            1526400896374L);
        checkAndValidateDateFormat(new TimestampMatch(1, "", "YYYY-MM-dd HH:mm:ss,SSSZ",
                "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}", "TIMESTAMP_ISO8601", ""), "2018-05-15 17:14:56,374+0100",
            1526400896374L);
        checkAndValidateDateFormat(new TimestampMatch(2, "", "YYYY-MM-dd HH:mm:ss,SSSZZ",
                "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}", "TIMESTAMP_ISO8601", ""), "2018-05-15 17:14:56,374+01:00",
            1526400896374L);
        checkAndValidateDateFormat(new TimestampMatch(3, "", "YYYY-MM-dd HH:mm:ss,SSS",
                "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}", "TIMESTAMP_ISO8601", ""), "2018-05-15 17:14:56,374", 1526400896374L);
        checkAndValidateDateFormat(new TimestampMatch(4, "", "YYYY-MM-dd HH:mm:ssZ",
                "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}", "TIMESTAMP_ISO8601", ""), "2018-05-15 16:14:56Z", 1526400896000L);
        checkAndValidateDateFormat(new TimestampMatch(4, "", "YYYY-MM-dd HH:mm:ssZ",
                "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}", "TIMESTAMP_ISO8601", ""), "2018-05-15 17:14:56+0100", 1526400896000L);
        checkAndValidateDateFormat(new TimestampMatch(5, "", "YYYY-MM-dd HH:mm:ssZZ",
                "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}", "TIMESTAMP_ISO8601", ""), "2018-05-15 17:14:56+01:00", 1526400896000L);
        checkAndValidateDateFormat(new TimestampMatch(6, "", "YYYY-MM-dd HH:mm:ss",
                "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}", "TIMESTAMP_ISO8601", ""), "2018-05-15 17:14:56", 1526400896000L);
    }

    public void testFindFirstMatchGivenOnlyKnownDateFormat() {

        // Note: some of the time formats give millisecond accuracy, some second accuracy and some minute accuracy

        checkAndValidateDateFormat(new TimestampMatch(0, "", "YYYY-MM-dd HH:mm:ss,SSS Z",
                "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}", "TOMCAT_DATESTAMP", ""), "2018-05-15 17:14:56,374 +0100",
            1526400896374L);

        checkAndValidateDateFormat(new TimestampMatch(8, "", "EEE MMM dd YYYY HH:mm:ss zzz",
                "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{4} \\d{2}:\\d{2}:\\d{2} ", "DATESTAMP_RFC822", ""),
            "Tue May 15 2018 16:14:56 UTC", 1526400896000L);
        checkAndValidateDateFormat(new TimestampMatch(9, "", "EEE MMM dd YYYY HH:mm zzz",
                "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{4} \\d{2}:\\d{2} ", "DATESTAMP_RFC822", ""),
            "Tue May 15 2018 16:14 UTC", 1526400840000L);

        checkAndValidateDateFormat(new TimestampMatch(10, "", "EEE, dd MMM YYYY HH:mm:ss ZZ",
                "\\b[A-Z]\\S{2,8}, \\d{1,2} [A-Z]\\S{2,8} \\d{4} \\d{2}:\\d{2}:\\d{2} ", "DATESTAMP_RFC2822", ""),
            "Tue, 15 May 2018 17:14:56 +01:00", 1526400896000L);
        checkAndValidateDateFormat(new TimestampMatch(11, "", "EEE, dd MMM YYYY HH:mm:ss Z",
                "\\b[A-Z]\\S{2,8}, \\d{1,2} [A-Z]\\S{2,8} \\d{4} \\d{2}:\\d{2}:\\d{2} ", "DATESTAMP_RFC2822", ""),
            "Tue, 15 May 2018 17:14:56 +0100", 1526400896000L);
        checkAndValidateDateFormat(new TimestampMatch(12, "", "EEE, dd MMM YYYY HH:mm ZZ",
                "\\b[A-Z]\\S{2,8}, \\d{1,2} [A-Z]\\S{2,8} \\d{4} \\d{2}:\\d{2} ", "DATESTAMP_RFC2822", ""),
            "Tue, 15 May 2018 17:14 +01:00", 1526400840000L);
        checkAndValidateDateFormat(new TimestampMatch(13, "", "EEE, dd MMM YYYY HH:mm Z",
                "\\b[A-Z]\\S{2,8}, \\d{1,2} [A-Z]\\S{2,8} \\d{4} \\d{2}:\\d{2} ", "DATESTAMP_RFC2822", ""), "Tue, 15 May 2018 17:14 +0100",
            1526400840000L);

        checkAndValidateDateFormat(new TimestampMatch(14, "", "EEE MMM dd HH:mm:ss zzz YYYY",
                "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{2}:\\d{2}:\\d{2} [A-Z]{3,4} \\d{4}\\b", "DATESTAMP_OTHER", ""),
            "Tue May 15 16:14:56 UTC 2018", 1526400896000L);
        checkAndValidateDateFormat(new TimestampMatch(15, "", "EEE MMM dd HH:mm zzz YYYY",
                "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{2}:\\d{2} [A-Z]{3,4} \\d{4}\\b", "DATESTAMP_OTHER", ""),
            "Tue May 15 16:14 UTC 2018", 1526400840000L);

        checkAndValidateDateFormat(new TimestampMatch(16, "", "YYYYMMddHHmmss", "\\b\\d{14}\\b", "DATESTAMP_EVENTLOG", ""),
            "20180515171456", 1526400896000L);

        checkAndValidateDateFormat(new TimestampMatch(17, "", "EEE MMM dd HH:mm:ss YYYY",
                "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{2}:\\d{2}:\\d{2} \\d{4}\\b", "HTTPDERROR_DATE", ""),
            "Tue May 15 17:14:56 2018", 1526400896000L);

        checkAndValidateDateFormat(new TimestampMatch(18, "", Arrays.asList("MMM dd HH:mm:ss.SSS", "MMM  d HH:mm:ss.SSS"),
            "\\b[A-Z]\\S{2,8} {1,2}\\d{1,2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}", "SYSLOGTIMESTAMP", ""), "May 15 17:14:56.725", 1526400896725L);
        checkAndValidateDateFormat(new TimestampMatch(19, "", Arrays.asList("MMM dd HH:mm:ss", "MMM  d HH:mm:ss"),
            "\\b[A-Z]\\S{2,8} {1,2}\\d{1,2} \\d{2}:\\d{2}:\\d{2}\\b", "SYSLOGTIMESTAMP", ""), "May 15 17:14:56", 1526400896000L);

        checkAndValidateDateFormat(new TimestampMatch(20, "", "dd/MMM/YYYY:HH:mm:ss Z",
                "\\b\\d{2}/[A-Z]\\S{2}/\\d{4}:\\d{2}:\\d{2}:\\d{2} ", "HTTPDATE", ""), "15/May/2018:17:14:56 +0100", 1526400896000L);

        checkAndValidateDateFormat(new TimestampMatch(21, "", "MMM dd, YYYY K:mm:ss a",
                "\\b[A-Z]\\S{2,8} \\d{1,2}, \\d{4} \\d{1,2}:\\d{2}:\\d{2} [AP]M\\b", "CATALINA_DATESTAMP", ""), "May 15, 2018 5:14:56 PM",
            1526400896000L);

        checkAndValidateDateFormat(new TimestampMatch(22, "", Arrays.asList("MMM dd YYYY HH:mm:ss", "MMM  d YYYY HH:mm:ss"),
                "\\b[A-Z]\\S{2,8} {1,2}\\d{1,2} \\d{4} \\d{2}:\\d{2}:\\d{2}\\b", "CISCOTIMESTAMP", ""), "May 15 2018 17:14:56",
            1526400896000L);
    }

    public void testFindFirstMatchGivenOnlySystemDate() {

        assertEquals(new TimestampMatch(23, "", "UNIX_MS", "\\b\\d{13}\\b", "POSINT", ""),
            TimestampFormatFinder.findFirstMatch("1526400896374"));
        assertEquals(new TimestampMatch(23, "", "UNIX_MS", "\\b\\d{13}\\b", "POSINT", ""),
            TimestampFormatFinder.findFirstFullMatch("1526400896374"));

        assertEquals(new TimestampMatch(24, "", "UNIX", "\\b\\d{10}\\.\\d{3,9}\\b", "NUMBER", ""),
            TimestampFormatFinder.findFirstMatch("1526400896.736"));
        assertEquals(new TimestampMatch(24, "", "UNIX", "\\b\\d{10}\\.\\d{3,9}\\b", "NUMBER", ""),
            TimestampFormatFinder.findFirstFullMatch("1526400896.736"));
        assertEquals(new TimestampMatch(25, "", "UNIX", "\\b\\d{10}\\b", "POSINT", ""),
            TimestampFormatFinder.findFirstMatch("1526400896"));
        assertEquals(new TimestampMatch(25, "", "UNIX", "\\b\\d{10}\\b", "POSINT", ""),
            TimestampFormatFinder.findFirstFullMatch("1526400896"));

        assertEquals(new TimestampMatch(26, "", "TAI64N", "\\b[0-9A-Fa-f]{24}\\b", "BASE16NUM", ""),
            TimestampFormatFinder.findFirstMatch("400000005afb159a164ac980"));
        assertEquals(new TimestampMatch(26, "", "TAI64N", "\\b[0-9A-Fa-f]{24}\\b", "BASE16NUM", ""),
            TimestampFormatFinder.findFirstFullMatch("400000005afb159a164ac980"));
    }

    private void checkAndValidateDateFormat(TimestampMatch expected, String text, long expectedEpochMs) {

        assertEquals(expected, TimestampFormatFinder.findFirstMatch(text));
        assertEquals(expected, TimestampFormatFinder.findFirstFullMatch(text));

        // All the test times are for Tue May 15 2018 16:14:56 UTC, which is 17:14:56 in London
        DateTimeZone zone = DateTimeZone.forID("Europe/London");
        DateTime parsed;
        for (int i = 0; i < expected.dateFormats.size(); ++i) {
            try {
                String dateFormat = expected.dateFormats.get(i);
                switch (dateFormat) {
                    case "ISO8601":
                        parsed = ISODateTimeFormat.dateTimeParser().withZone(zone).withDefaultYear(2018).parseDateTime(text);
                        break;
                    default:
                        DateTimeFormatter parser = DateTimeFormat.forPattern(dateFormat).withZone(zone).withLocale(Locale.UK);
                        parsed = parser.withDefaultYear(2018).parseDateTime(text);
                        break;
                }
                if (expectedEpochMs == parsed.getMillis()) {
                    break;
                }
                // If the last one isn't right then propagate
                if (i == expected.dateFormats.size() - 1) {
                    assertEquals(expectedEpochMs, parsed.getMillis());
                }
            } catch (RuntimeException e) {
                // If the last one throws then propagate
                if (i == expected.dateFormats.size() - 1) {
                    throw e;
                }
            }
        }
        assertTrue(expected.simplePattern.matcher(text).find());
    }

    public void testFindFirstMatchGivenRealLogMessages() {

        assertEquals(new TimestampMatch(7, "[", "ISO8601", "\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}", "TIMESTAMP_ISO8601",
                "][INFO ][o.e.e.NodeEnvironment    ] [node-0] heap size [3.9gb], compressed ordinary object pointers [true]"),
            TimestampFormatFinder.findFirstMatch("[2018-05-11T17:07:29,553][INFO ][o.e.e.NodeEnvironment    ] [node-0] " +
                "heap size [3.9gb], compressed ordinary object pointers [true]"));

        assertEquals(new TimestampMatch(20, "192.168.62.101 - - [", "dd/MMM/YYYY:HH:mm:ss Z",
                "\\b\\d{2}/[A-Z]\\S{2}/\\d{4}:\\d{2}:\\d{2}:\\d{2} ", "HTTPDATE",
                "] \"POST //apiserv:8080/engine/v2/jobs HTTP/1.1\" 201 42 \"-\" \"curl/7.46.0\" 384"),
            TimestampFormatFinder.findFirstMatch("192.168.62.101 - - [29/Jun/2016:12:11:31 +0000] " +
                "\"POST //apiserv:8080/engine/v2/jobs HTTP/1.1\" 201 42 \"-\" \"curl/7.46.0\" 384"));

        assertEquals(new TimestampMatch(21, "", "MMM dd, YYYY K:mm:ss a",
                "\\b[A-Z]\\S{2,8} \\d{1,2}, \\d{4} \\d{1,2}:\\d{2}:\\d{2} [AP]M\\b", "CATALINA_DATESTAMP",
                " org.apache.tomcat.util.http.Parameters processParameters"),
            TimestampFormatFinder.findFirstMatch("Aug 29, 2009 12:03:57 AM org.apache.tomcat.util.http.Parameters processParameters"));

        assertEquals(new TimestampMatch(19, "", Arrays.asList("MMM dd HH:mm:ss", "MMM  d HH:mm:ss"),
                "\\b[A-Z]\\S{2,8} {1,2}\\d{1,2} \\d{2}:\\d{2}:\\d{2}\\b", "SYSLOGTIMESTAMP", " esxi1.acme.com Vpxa: " +
                    "[3CB3FB90 verbose 'vpxavpxaInvtVm' opID=WFU-33d82c31] [VpxaInvtVmChangeListener] Guest DiskInfo Changed"),
            TimestampFormatFinder.findFirstMatch("Oct 19 17:04:44 esxi1.acme.com Vpxa: [3CB3FB90 verbose 'vpxavpxaInvtVm' " +
                "opID=WFU-33d82c31] [VpxaInvtVmChangeListener] Guest DiskInfo Changed"));

        assertEquals(new TimestampMatch(7, "559550912540598297\t", "ISO8601", "\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}",
                "TIMESTAMP_ISO8601",
                "\t2016-04-20T21:06:53Z\t38545844\tserv02nw07\t192.168.114.28\tAuthpriv\tInfo\tsshd\tsubsystem request for sftp"),
            TimestampFormatFinder.findFirstMatch("559550912540598297\t2016-04-20T14:06:53\t2016-04-20T21:06:53Z\t38545844\tserv02nw07\t" +
                "192.168.114.28\tAuthpriv\tInfo\tsshd\tsubsystem request for sftp"));

        assertEquals(new TimestampMatch(19, "", Arrays.asList("MMM dd HH:mm:ss", "MMM  d HH:mm:ss"),
                "\\b[A-Z]\\S{2,8} {1,2}\\d{1,2} \\d{2}:\\d{2}:\\d{2}\\b", "SYSLOGTIMESTAMP",
                " dnsserv named[22529]: error (unexpected RCODE REFUSED) resolving 'www.elastic.co/A/IN': 95.110.68.206#53"),
            TimestampFormatFinder.findFirstMatch("Sep  8 11:55:35 dnsserv named[22529]: error (unexpected RCODE REFUSED) resolving " +
                "'www.elastic.co/A/IN': 95.110.68.206#53"));

        assertEquals(new TimestampMatch(3, "", "YYYY-MM-dd HH:mm:ss.SSSSSS", "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}",
                "TIMESTAMP_ISO8601",
                "|INFO    |VirtualServer |1  |client  'User1'(id:2) was added to channelgroup 'Channel Admin'(id:5) by client " +
                    "'User1'(id:2) in channel '3er Instanz'(id:2)"),
            TimestampFormatFinder.findFirstMatch("2018-01-06 19:22:20.106822|INFO    |VirtualServer |1  |client " +
                " 'User1'(id:2) was added to channelgroup 'Channel Admin'(id:5) by client 'User1'(id:2) in channel '3er Instanz'(id:2)"));
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
}
