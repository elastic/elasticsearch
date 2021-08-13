/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import java.time.ZoneId;
import java.time.ZonedDateTime;

public class DateTimeTests extends ScriptTestCase {

    public void testLongToZonedDateTime() {
        assertEquals(ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of("Z")), exec(
                "long milliSinceEpoch = 434931330000L;" +
                "Instant instant = Instant.ofEpochMilli(milliSinceEpoch);" +
                "return ZonedDateTime.ofInstant(instant, ZoneId.of('Z'));"
        ));
    }

    public void testStringToZonedDateTime() {
        assertEquals(ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of("Z")), exec(
                "String milliSinceEpochString = '434931330000';" +
                "long milliSinceEpoch = Long.parseLong(milliSinceEpochString);" +
                "Instant instant = Instant.ofEpochMilli(milliSinceEpoch);" +
                "return ZonedDateTime.ofInstant(instant, ZoneId.of('Z'));"
        ));

        assertEquals(ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of("Z")), exec(
                "String datetime = '1983-10-13T22:15:30Z';" +
                "return ZonedDateTime.parse(datetime);"
        ));

        assertEquals(ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of("Z")), exec(
                "String datetime = 'Thu, 13 Oct 1983 22:15:30 GMT';" +
                "return ZonedDateTime.parse(datetime, DateTimeFormatter.RFC_1123_DATE_TIME);"
        ));

        assertEquals(ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of("Z")), exec(
                "String datetime = 'custom y 1983 m 10 d 13 22:15:30 Z';" +
                "DateTimeFormatter dtf = DateTimeFormatter.ofPattern(" +
                        "\"'custom' 'y' yyyy 'm' MM 'd' dd HH:mm:ss VV\");" +
                "return ZonedDateTime.parse(datetime, dtf);"
        ));
    }

    public void testPiecesToZonedDateTime() {
        assertEquals(ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of("Z")), exec(
                "int year = 1983;" +
                "int month = 10;" +
                "int day = 13;" +
                "int hour = 22;" +
                "int minutes = 15;" +
                "int seconds = 30;" +
                "int nanos = 0;" +
                "String tz = 'Z';" +
                "return ZonedDateTime.of(year, month, day, hour, minutes, seconds, nanos, ZoneId.of(tz));"
        ));
    }

    public void testZonedDatetimeToLong() {
        assertEquals(434931330000L, exec(
                "ZonedDateTime zdt = ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));" +
                "return zdt.toInstant().toEpochMilli();"
        ));
    }

    public void testZonedDateTimeToString() {
        assertEquals("1983-10-13T22:15:30Z", exec(
                "ZonedDateTime zdt = ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));" +
                "return zdt.format(DateTimeFormatter.ISO_INSTANT);"
        ));

        assertEquals("date: 1983/10/13 time: 22:15:30", exec(
                "ZonedDateTime zdt = ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));" +
                "DateTimeFormatter dtf = DateTimeFormatter.ofPattern(" +
                    "\"'date:' yyyy/MM/dd 'time:' HH:mm:ss\");" +
                "return zdt.format(dtf);"
        ));
    }

    public void testZonedDateTimeToPieces() {
        assertArrayEquals(new int[] {1983, 10, 13, 22, 15, 30, 100}, (int[])exec(
                "int[] pieces = new int[7];" +
                "ZonedDateTime zdt = ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 100, ZoneId.of('Z'));" +
                "pieces[0] = zdt.year;" +
                "pieces[1] = zdt.monthValue;" +
                "pieces[2] = zdt.dayOfMonth;" +
                "pieces[3] = zdt.hour;" +
                "pieces[4] = zdt.minute;" +
                "pieces[5] = zdt.second;" +
                "pieces[6] = zdt.nano;" +
                "return pieces;"
        ));
    }

    public void testLongManipulation() {
        assertEquals(ZonedDateTime.of(1983, 10, 13, 22, 15, 27, 0, ZoneId.of("Z")), exec(
                "long milliSinceEpoch = 434931330000L;" +
                "milliSinceEpoch = milliSinceEpoch - 1000L*3L;" +
                "Instant instant = Instant.ofEpochMilli(milliSinceEpoch);" +
                "return ZonedDateTime.ofInstant(instant, ZoneId.of('Z'))"
        ));
    }

    public void testZonedDateTimeManipulation() {
        assertEquals(ZonedDateTime.of(1983, 10, 16, 22, 15, 30, 0, ZoneId.of("Z")), exec(
                "ZonedDateTime zdt = ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));" +
                "return zdt.plusDays(3);"
        ));

        assertEquals(ZonedDateTime.of(1983, 10, 13, 20, 10, 30, 0, ZoneId.of("Z")), exec(
                "ZonedDateTime zdt = ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));" +
                "return zdt.minusMinutes(125);"
        ));

        assertEquals(ZonedDateTime.of(1976, 10, 13, 22, 15, 30, 0, ZoneId.of("Z")), exec(
                "ZonedDateTime zdt = ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));" +
                "return zdt.withYear(1976);"
        ));
    }

    public void testLongTimeDifference() {
        assertEquals(3000L, exec(
                "long startTimestamp = 434931327000L;" +
                "long endTimestamp = 434931330000L;" +
                "return endTimestamp - startTimestamp;"
        ));
    }

    public void testZonedDateTimeDifference() {
        assertEquals(4989L, exec(
                "ZonedDateTime zdt1 = ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 11000000, ZoneId.of('Z'));" +
                "ZonedDateTime zdt2 = ZonedDateTime.of(1983, 10, 13, 22, 15, 35, 0, ZoneId.of('Z'));" +
                "return ChronoUnit.MILLIS.between(zdt1, zdt2);"
        ));

        assertEquals(4L, exec(
                "ZonedDateTime zdt1 = ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 11000000, ZoneId.of('Z'));" +
                "ZonedDateTime zdt2 = ZonedDateTime.of(1983, 10, 17, 22, 15, 35, 0, ZoneId.of('Z'));" +
                "return ChronoUnit.DAYS.between(zdt1, zdt2);"
        ));
    }

    public void compareLongs() {
        assertEquals(false, exec(
                "long ts1 = 434931327000L;" +
                "long ts2 = 434931330000L;" +
                "return ts1 > ts2;"
        ));
    }

    public void compareZonedDateTimes() {
        assertEquals(true, exec(
                "ZonedDateTime zdt1 = ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));" +
                "ZonedDateTime zdt2 = ZonedDateTime.of(1983, 10, 17, 22, 15, 35, 0, ZoneId.of('Z'));" +
                "return zdt1.isBefore(zdt2);"
        ));

        assertEquals(false, exec(
                "ZonedDateTime zdt1 = ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));" +
                "ZonedDateTime zdt2 = ZonedDateTime.of(1983, 10, 17, 22, 15, 35, 0, ZoneId.of('Z'));" +
                "return zdt1.isAfter(zdt2);"
        ));
    }

    public void testTimeZone() {
        assertEquals(ZonedDateTime.of(1983, 10, 13, 15, 15, 30, 0, ZoneId.of("America/Los_Angeles")), exec(
                "ZonedDateTime utc = ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));" +
                "return utc.withZoneSameInstant(ZoneId.of('America/Los_Angeles'));"));

        assertEquals("Thu, 13 Oct 1983 15:15:30 -0700", exec(
                "String gmtString = 'Thu, 13 Oct 1983 22:15:30 GMT';" +
                "ZonedDateTime gmtZdt = ZonedDateTime.parse(gmtString," +
                "DateTimeFormatter.RFC_1123_DATE_TIME);" +
                "ZonedDateTime pstZdt =" +
                "gmtZdt.withZoneSameInstant(ZoneId.of('America/Los_Angeles'));" +
                "return pstZdt.format(DateTimeFormatter.RFC_1123_DATE_TIME);"));
    }
}
