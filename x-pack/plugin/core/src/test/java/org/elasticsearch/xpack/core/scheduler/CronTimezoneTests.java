/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.scheduler;

import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;

import static java.util.TimeZone.getTimeZone;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;

public class CronTimezoneTests extends ESTestCase {

    public void testForFixedOffsetCorrectlyCalculateNextRuntime() {
        Cron cron = new Cron("0 0 2 * * ?", getTimeZone(ZoneOffset.of("+1")));
        long midnightUTC = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        long nextValidTimeAfter = cron.getNextValidTimeAfter(midnightUTC);
        assertThat(nextValidTimeAfter, equalTo(Instant.parse("2020-01-01T01:00:00Z").toEpochMilli()));
    }

    public void testForFixedOffsetLongDateCorrectlyCalculateNextRuntime() {
        Cron cron = new Cron("0 0 1 1 1 ?", getTimeZone(ZoneOffset.of("+1")));
        long midnightUTC = Instant.parse("2020-01-01T00:00:01Z").toEpochMilli();
        long nextValidTimeAfter = cron.getNextValidTimeAfter(midnightUTC);
        assertThat(nextValidTimeAfter, equalTo(Instant.parse("2021-01-01T00:00:00Z").toEpochMilli()));
    }

    public void testForLondonFixedDSTTransitionCheckCorrectSchedule() {
        ZoneId londonZone = getTimeZone("Europe/London").toZoneId();

        Cron cron = new Cron("0 0 2 * * ?", getTimeZone(londonZone));
        ZoneRules londonZoneRules = londonZone.getRules();
        Instant springMidnight = Instant.parse("2020-03-01T00:00:00Z");
        long timeBeforeDST = springMidnight.toEpochMilli();

        assertThat(cron.getNextValidTimeAfter(timeBeforeDST), equalTo(Instant.parse("2020-03-01T02:00:00Z").toEpochMilli()));

        ZoneOffsetTransition zoneOffsetTransition = londonZoneRules.nextTransition(springMidnight);

        Instant timeAfterDST = zoneOffsetTransition.getDateTimeBefore()
            .plusDays(1)
            .atZone(ZoneOffset.UTC)
            .withHour(0)
            .withMinute(0)
            .toInstant();

        assertThat(cron.getNextValidTimeAfter(timeAfterDST.toEpochMilli()), equalTo(Instant.parse("2020-03-30T01:00:00Z").toEpochMilli()));
    }

    public void testRandomDSTTransitionCalculateNextTimeCorrectlyRelativeToUTC() {
        ZoneId timeZone;

        int i = 0;
        boolean found;
        do {
            timeZone = randomZone();
            found = getTimeZone(timeZone).useDaylightTime();
            i++;
        } while (found == false && i <= 500); // Infinite loop prevention

        if (found == false) {
            fail("Could not find a timezone with DST");
        }

        logger.info("Testing for timezone {}", timeZone);

        ZoneOffsetTransition zoneOffsetTransition = timeZone.getRules().nextTransition(Instant.now());

        ZonedDateTime midnightBefore = zoneOffsetTransition.getDateTimeBefore().atZone(timeZone).minusDays(2).withHour(0).withMinute(0);
        ZonedDateTime midnightAfter = zoneOffsetTransition.getDateTimeAfter().atZone(timeZone).plusDays(2).withHour(0).withMinute(0);

        long epochBefore = midnightBefore.toInstant().toEpochMilli();
        long epochAfter = midnightAfter.toInstant().toEpochMilli();

        Cron cron = new Cron("0 0 2 * * ?", getTimeZone(timeZone));

        long nextScheduleBefore = cron.getNextValidTimeAfter(epochBefore);
        long nextScheduleAfter = cron.getNextValidTimeAfter(epochAfter);

        assertThat(nextScheduleBefore - epochBefore, equalTo(2 * 60 * 60 * 1000L));
        assertThat(nextScheduleAfter - epochAfter, equalTo(2 * 60 * 60 * 1000L));

        ZonedDateTime utcMidnightBefore = zoneOffsetTransition.getDateTimeBefore()
            .atZone(ZoneOffset.UTC)
            .minusDays(2)
            .withHour(0)
            .withMinute(0);

        ZonedDateTime utcMidnightAfter = zoneOffsetTransition.getDateTimeAfter()
            .atZone(ZoneOffset.UTC)
            .plusDays(2)
            .withHour(0)
            .withMinute(0);

        long utcEpochBefore = utcMidnightBefore.toInstant().toEpochMilli();
        long utcEpochAfter = utcMidnightAfter.toInstant().toEpochMilli();

        long nextUtcScheduleBefore = cron.getNextValidTimeAfter(utcEpochBefore);
        long nextUtcScheduleAfter = cron.getNextValidTimeAfter(utcEpochAfter);

        assertThat(nextUtcScheduleBefore - utcEpochBefore, not(equalTo(nextUtcScheduleAfter - utcEpochAfter)));

    }

    public void testForGMTAdvanceTransitionTriggerTimeIsRoundedToAfterDiscontinuity() {
        ZoneId london = ZoneId.of("Europe/London");
        Cron cron = new Cron("0 30 1 * * ?", getTimeZone(london));

        Instant beforeTransition = Instant.parse("2025-03-30T00:00:00Z");
        long beforeTransitionEpoch = beforeTransition.toEpochMilli();

        long nextValidTimeAfter = cron.getNextValidTimeAfter(beforeTransitionEpoch);
        System.out.println("nextValidTimeAfter = " + nextValidTimeAfter);
        assertThat(nextValidTimeAfter, equalTo(Instant.parse("2025-03-30T01:00:00Z").toEpochMilli()));
    }

    public void testForGMTRetardTransitionTriggerTimeIsRoundedToAfterDiscontinuity() {
        ZoneId london = ZoneId.of("Europe/London");
        Cron cron = new Cron("0 30 1 * * ?", getTimeZone(london));

        Instant beforeTransition = Instant.parse("2024-10-27T00:00:00Z");
        long beforeTransitionEpoch = beforeTransition.toEpochMilli();

        long firstValidTimeAfter = cron.getNextValidTimeAfter(beforeTransitionEpoch);
        System.out.println("nextValidTimeAfter = " + firstValidTimeAfter);
        assertThat(firstValidTimeAfter, equalTo(Instant.parse("2024-10-27T00:30:00Z").toEpochMilli()));

        long nextValidTimeAfter = cron.getNextValidTimeAfter(firstValidTimeAfter);
        System.out.println("nextValidTimeAfter = " + nextValidTimeAfter);
        assertThat(nextValidTimeAfter, equalTo(Instant.parse("2024-10-28T01:30:00Z").toEpochMilli()));
    }

}
