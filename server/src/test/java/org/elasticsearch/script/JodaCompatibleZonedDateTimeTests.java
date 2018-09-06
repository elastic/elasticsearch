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

    // Thursday, September 6, 2018 3:01:20.617 PM GMT-07:00 DST
    private static JodaCompatibleZonedDateTime TIME =
        new JodaCompatibleZonedDateTime(Instant.ofEpochMilli(1536271280617L), ZoneOffset.ofHours(-7));

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

    void assertMethodDeprecation(Runnable assertions, String methodName) {
        assertDeprecation(assertions, "Use of joda time method [" + methodName + "] is deprecated. Use the java time api instead.");
    }

    public void testDayOfMonth() {
        assertThat(TIME.getDayOfMonth(), equalTo(6));
    }

    public void testDayOfYear() {
        assertThat(TIME.getDayOfYear(), equalTo(249));
    }

    public void testHour() {
        assertThat(TIME.getHour(), equalTo(15));
    }

    public void testLocalDate() {
        assertThat(TIME.toLocalDate(), equalTo(LocalDate.of(2018, 9, 6)));
    }

    public void testLocalDateTime() {
        assertThat(TIME.toLocalDateTime(), equalTo(LocalDateTime.of(2018, 9, 6, 15, 1, 20, 617000000)));
    }

    public void testMinute() {
        assertThat(TIME.getMinute(), equalTo(1));
    }

    public void testMonth() {
        assertThat(TIME.getMonth(), equalTo(Month.SEPTEMBER));
    }

    public void testMonthValue() {
        assertThat(TIME.getMonthValue(), equalTo(9));
    }

    public void testNano() {
        assertThat(TIME.getNano(), equalTo(617000000));
    }

    public void testSecond() {
        assertThat(TIME.getSecond(), equalTo(20));
    }

    public void testYear() {
        assertThat(TIME.getYear(), equalTo(2018));
    }

    public void testMillis() {
        assertMethodDeprecation(() -> assertThat(TIME.getMillis(), equalTo(1536271280617L)), "getMillis()");
    }

    public void testCenturyOfEra() {
        assertMethodDeprecation(() -> assertThat(TIME.getCenturyOfEra(), equalTo(20)), "getCenturyOfEra()");
    }

    public void testEra() {
        assertMethodDeprecation(() -> assertThat(TIME.getEra(), equalTo(1)), "getEra()");
    }

    public void testHourOfDay() {
        assertMethodDeprecation(() -> assertThat(TIME.getHourOfDay(), equalTo(15)), "getHourOfDay()");
    }

    public void testMillisOfDay() {
        assertMethodDeprecation(() -> assertThat(TIME.getMillisOfDay(), equalTo(54080617)), "getMillisOfDay()");
    }

    public void testMillisOfSecond() {
        assertMethodDeprecation(() -> assertThat(TIME.getMillisOfSecond(), equalTo(617)), "getMillisOfSecond()");
    }

    public void testMinuteOfDay() {
        assertMethodDeprecation(() -> assertThat(TIME.getMinuteOfDay(), equalTo(901)), "getMinuteOfDay()");
    }

    public void testMinuteOfHour() {
        assertMethodDeprecation(() -> assertThat(TIME.getMinuteOfHour(), equalTo(1)), "getMinuteOfHour()");
    }

    public void testMonthOfYear() {
        assertMethodDeprecation(() -> assertThat(TIME.getMonthOfYear(), equalTo(9)), "getMonthOfYear()");
    }

    public void testSecondOfDay() {
        assertMethodDeprecation(() -> assertThat(TIME.getSecondOfDay(), equalTo(54080)), "getSecondOfDay()");
    }

    public void testSecondOfMinute() {
        assertMethodDeprecation(() -> assertThat(TIME.getSecondOfMinute(), equalTo(20)), "getSecondOfMinute()");
    }

    public void testWeekOfWeekyear() {
        assertMethodDeprecation(() -> assertThat(TIME.getWeekOfWeekyear(), equalTo(36)), "getWeekOfWeekyear()");
    }

    public void testWeekyear() {
        assertMethodDeprecation(() -> assertThat(TIME.getWeekyear(), equalTo(2018)), "getWeekyear()");
    }

    public void testYearOfCentury() {
        assertMethodDeprecation(() -> assertThat(TIME.getYearOfCentury(), equalTo(18)), "getYearOfCentury()");
    }

    public void testYearOfEra() {
        assertMethodDeprecation(() -> assertThat(TIME.getYearOfEra(), equalTo(2018)), "getYearOfEra()");
    }

    public void testToString1() {
        assertMethodDeprecation(() -> assertThat(TIME.toString("MM-dd-YYYY"), equalTo("09-06-2018")), "toString(String)");
    }

    public void testToString2() {
        assertMethodDeprecation(() -> assertThat(TIME.toString("EEE", Locale.GERMANY), equalTo("Do.")),
            "toString(String,Locale)");
    }

    public void testDayOfWeek() {
        assertDeprecation(() -> assertThat(TIME.getDayOfWeek(), equalTo(4)),
            "getDayOfWeek() is incompatible with the java time api. Use getDayOfWeekEnum().getValue().");
    }

    public void testDayOfWeekEnum() {
        assertThat(TIME.getDayOfWeekEnum(), equalTo(DayOfWeek.THURSDAY));
    }
}
