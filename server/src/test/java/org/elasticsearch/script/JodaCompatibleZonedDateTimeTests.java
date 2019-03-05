/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.script;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;

public class JodaCompatibleZonedDateTimeTests extends ESTestCase {
    private static final Logger DEPRECATION_LOGGER =
        LogManager.getLogger("org.elasticsearch.deprecation.script.JodaCompatibleZonedDateTime");

    // each call to get or getValue will be run with limited permissions, just as they are in scripts
    private static PermissionCollection NO_PERMISSIONS = new Permissions();
    private static AccessControlContext NO_PERMISSIONS_ACC = new AccessControlContext(
        new ProtectionDomain[] {
            new ProtectionDomain(null, NO_PERMISSIONS)
        }
    );

    private JodaCompatibleZonedDateTime javaTime;
    private DateTime jodaTime;

    @Before
    public void setupTime() {
        long millis = randomIntBetween(0, Integer.MAX_VALUE);
        javaTime = new JodaCompatibleZonedDateTime(Instant.ofEpochMilli(millis), ZoneOffset.ofHours(-7));
        jodaTime = new DateTime(millis, DateTimeZone.forOffsetHours(-7));
    }

    void assertDeprecation(Runnable assertions, String message) {
        Appender appender = new AbstractAppender("test", null, null) {
            @Override
            public void append(LogEvent event) {
                /* Create a temporary directory to prove we are running with the
                 * server's permissions. */
                createTempDir();
            }
        };
        appender.start();
        Loggers.addAppender(DEPRECATION_LOGGER, appender);
        try {
            // the assertions are run with the same reduced privileges scripts run with
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                assertions.run();
                return null;
            }, NO_PERMISSIONS_ACC);
        } finally {
            appender.stop();
            Loggers.removeAppender(DEPRECATION_LOGGER, appender);
        }

        assertWarnings(message);
    }

    void assertMethodDeprecation(Runnable assertions, String oldMethod, String newMethod) {
        assertDeprecation(assertions, "Use of the joda time method [" + oldMethod + "] is deprecated. Use [" + newMethod + "] instead.");
    }

    public void testEquals() {
        assertThat(javaTime, equalTo(javaTime));
    }

    public void testToString() {
        assertThat(javaTime.toString(), equalTo(jodaTime.toString()));
    }

    public void testDayOfMonth() {
        assertThat(javaTime.getDayOfMonth(), equalTo(jodaTime.getDayOfMonth()));
    }

    public void testDayOfYear() {
        assertThat(javaTime.getDayOfYear(), equalTo(jodaTime.getDayOfYear()));
    }

    public void testHour() {
        assertThat(javaTime.getHour(), equalTo(jodaTime.getHourOfDay()));
    }

    public void testLocalDate() {
        assertThat(javaTime.toLocalDate(), equalTo(LocalDate.of(jodaTime.getYear(), jodaTime.getMonthOfYear(), jodaTime.getDayOfMonth())));
    }

    public void testLocalDateTime() {
        LocalDateTime dt = LocalDateTime.of(jodaTime.getYear(), jodaTime.getMonthOfYear(), jodaTime.getDayOfMonth(),
                                            jodaTime.getHourOfDay(), jodaTime.getMinuteOfHour(), jodaTime.getSecondOfMinute(),
                                            jodaTime.getMillisOfSecond() * 1000000);
        assertThat(javaTime.toLocalDateTime(), equalTo(dt));
    }

    public void testMinute() {
        assertThat(javaTime.getMinute(), equalTo(jodaTime.getMinuteOfHour()));
    }

    public void testMonth() {
        assertThat(javaTime.getMonth(), equalTo(Month.of(jodaTime.getMonthOfYear())));
    }

    public void testMonthValue() {
        assertThat(javaTime.getMonthValue(), equalTo(jodaTime.getMonthOfYear()));
    }

    public void testNano() {
        assertThat(javaTime.getNano(), equalTo(jodaTime.getMillisOfSecond() * 1000000));
    }

    public void testSecond() {
        assertThat(javaTime.getSecond(), equalTo(jodaTime.getSecondOfMinute()));
    }

    public void testYear() {
        assertThat(javaTime.getYear(), equalTo(jodaTime.getYear()));
    }

    public void testZone() {
        assertThat(javaTime.getZone().getId(), equalTo(jodaTime.getZone().getID()));
    }

    public void testMillis() {
        assertMethodDeprecation(() -> assertThat(javaTime.getMillis(), equalTo(jodaTime.getMillis())),
            "getMillis()", "toInstant().toEpochMilli()");
    }

    public void testCenturyOfEra() {
        assertMethodDeprecation(() -> assertThat(javaTime.getCenturyOfEra(), equalTo(jodaTime.getCenturyOfEra())),
            "getCenturyOfEra()", "get(ChronoField.YEAR_OF_ERA) / 100");
    }

    public void testEra() {
        assertMethodDeprecation(() -> assertThat(javaTime.getEra(), equalTo(jodaTime.getEra())),
            "getEra()", "get(ChronoField.ERA)");
    }

    public void testHourOfDay() {
        assertMethodDeprecation(() -> assertThat(javaTime.getHourOfDay(), equalTo(jodaTime.getHourOfDay())),
            "getHourOfDay()", "getHour()");
    }

    public void testMillisOfDay() {
        assertMethodDeprecation(() -> assertThat(javaTime.getMillisOfDay(), equalTo(jodaTime.getMillisOfDay())),
            "getMillisOfDay()", "get(ChronoField.MILLI_OF_DAY)");
    }

    public void testMillisOfSecond() {
        assertMethodDeprecation(() -> assertThat(javaTime.getMillisOfSecond(), equalTo(jodaTime.getMillisOfSecond())),
            "getMillisOfSecond()", "get(ChronoField.MILLI_OF_SECOND)");
    }

    public void testMinuteOfDay() {
        assertMethodDeprecation(() -> assertThat(javaTime.getMinuteOfDay(), equalTo(jodaTime.getMinuteOfDay())),
            "getMinuteOfDay()", "get(ChronoField.MINUTE_OF_DAY)");
    }

    public void testMinuteOfHour() {
        assertMethodDeprecation(() -> assertThat(javaTime.getMinuteOfHour(), equalTo(jodaTime.getMinuteOfHour())),
            "getMinuteOfHour()", "getMinute()");
    }

    public void testMonthOfYear() {
        assertMethodDeprecation(() -> assertThat(javaTime.getMonthOfYear(), equalTo(jodaTime.getMonthOfYear())),
            "getMonthOfYear()", "getMonthValue()");
    }

    public void testSecondOfDay() {
        assertMethodDeprecation(() -> assertThat(javaTime.getSecondOfDay(), equalTo(jodaTime.getSecondOfDay())),
            "getSecondOfDay()", "get(ChronoField.SECOND_OF_DAY)");
    }

    public void testSecondOfMinute() {
        assertMethodDeprecation(() -> assertThat(javaTime.getSecondOfMinute(), equalTo(jodaTime.getSecondOfMinute())),
            "getSecondOfMinute()", "getSecond()");
    }

    public void testWeekOfWeekyear() {
        assertMethodDeprecation(() -> assertThat(javaTime.getWeekOfWeekyear(), equalTo(jodaTime.getWeekOfWeekyear())),
            "getWeekOfWeekyear()", "get(WeekFields.ISO.weekOfWeekBasedYear())");
    }

    public void testWeekyear() {
        assertMethodDeprecation(() -> assertThat(javaTime.getWeekyear(), equalTo(jodaTime.getWeekyear())),
            "getWeekyear()", "get(WeekFields.ISO.weekBasedYear())");
    }

    public void testYearOfCentury() {
        assertMethodDeprecation(() -> assertThat(javaTime.getYearOfCentury(), equalTo(jodaTime.getYearOfCentury())),
            "getYearOfCentury()", "get(ChronoField.YEAR_OF_ERA) % 100");
    }

    public void testYearOfEra() {
        assertMethodDeprecation(() -> assertThat(javaTime.getYearOfEra(), equalTo(jodaTime.getYearOfEra())),
            "getYearOfEra()", "get(ChronoField.YEAR_OF_ERA)");
    }

    public void testToString1() {
        assertMethodDeprecation(() -> assertThat(javaTime.toString("YYYY/MM/dd HH:mm:ss.SSS"),
            equalTo(jodaTime.toString("YYYY/MM/dd HH:mm:ss.SSS"))), "toString(String)", "a DateTimeFormatter");
    }

    public void testToString2() {
        assertMethodDeprecation(() -> assertThat(javaTime.toString("EEE", Locale.GERMANY),
            equalTo(jodaTime.toString("EEE", Locale.GERMANY))), "toString(String,Locale)", "a DateTimeFormatter");
    }

    public void testDayOfWeek() {
        assertDeprecation(() -> assertThat(javaTime.getDayOfWeek(), equalTo(jodaTime.getDayOfWeek())),
            "The return type of [getDayOfWeek()] will change to an enum in 7.0. Use getDayOfWeekEnum().getValue().");
    }

    public void testDayOfWeekEnum() {
        assertThat(javaTime.getDayOfWeekEnum(), equalTo(DayOfWeek.of(jodaTime.getDayOfWeek())));
    }

    public void testToStringWithLocaleAndZeroOffset() {
        JodaCompatibleZonedDateTime dt = new JodaCompatibleZonedDateTime(Instant.EPOCH, ZoneOffset.ofTotalSeconds(0));
        assertMethodDeprecation(() -> dt.toString("yyyy-MM-dd hh:mm", Locale.ROOT), "toString(String,Locale)", "a DateTimeFormatter");
    }

    public void testToStringAndZeroOffset() {
        JodaCompatibleZonedDateTime dt = new JodaCompatibleZonedDateTime(Instant.EPOCH, ZoneOffset.ofTotalSeconds(0));
        assertMethodDeprecation(() -> dt.toString("yyyy-MM-dd hh:mm"), "toString(String)", "a DateTimeFormatter");
    }

    public void testIsEqual() {
        assertTrue(javaTime.isEqual(javaTime));
    }

    public void testIsAfter() {
        long millis = randomLongBetween(0, Integer.MAX_VALUE / 2);
        JodaCompatibleZonedDateTime beforeTime = new JodaCompatibleZonedDateTime(Instant.ofEpochMilli(millis), ZoneOffset.ofHours(-7));
        millis = randomLongBetween(millis + 1, Integer.MAX_VALUE);
        JodaCompatibleZonedDateTime afterTime = new JodaCompatibleZonedDateTime(Instant.ofEpochMilli(millis), ZoneOffset.ofHours(-7));
        assertTrue(afterTime.isAfter(beforeTime));
    }
    public void testIsBefore() {
        long millis = randomLongBetween(0, Integer.MAX_VALUE / 2);
        JodaCompatibleZonedDateTime beforeTime = new JodaCompatibleZonedDateTime(Instant.ofEpochMilli(millis), ZoneOffset.ofHours(-7));
        millis = randomLongBetween(millis + 1, Integer.MAX_VALUE);
        JodaCompatibleZonedDateTime afterTime = new JodaCompatibleZonedDateTime(Instant.ofEpochMilli(millis), ZoneOffset.ofHours(-7));
        assertTrue(beforeTime.isBefore(afterTime));
    }
}
