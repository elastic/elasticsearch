/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.scheduler;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;

import static java.util.TimeZone.getTimeZone;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;

public class CronTimezoneTests extends ESTestCase {

    public void testForFixedOffsetCorrectlyCalculateNextRuntime() {
        Cron cron = new Cron("0 0 2 * * ?", getTimeZone(ZoneOffset.of("+1"))); // Every day at 2:00
        long midnightUTC = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        long nextValidTimeAfter = cron.getNextValidTimeAfter(midnightUTC);
        assertThat(nextValidTimeAfter, equalTo(Instant.parse("2020-01-01T01:00:00Z").toEpochMilli()));
    }

    public void testForFixedOffsetLongDateCorrectlyCalculateNextRuntime() {
        Cron cron = new Cron("0 0 1 1 1 ?", getTimeZone(ZoneOffset.of("+1"))); // Every year at 1:00 on 1st January
        long midnightUTC = Instant.parse("2020-01-01T00:00:01Z").toEpochMilli();
        long nextValidTimeAfter = cron.getNextValidTimeAfter(midnightUTC);
        assertThat(nextValidTimeAfter, equalTo(Instant.parse("2021-01-01T00:00:00Z").toEpochMilli()));
    }

    public void testForLondonFixedDSTTransitionCheckCorrectSchedule() {
        ZoneId londonZone = getTimeZone("Europe/London").toZoneId();

        Cron cron = new Cron("0 0 2 * * ?", getTimeZone(londonZone)); // Every day at 2:00
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
        ZoneId timeZone = generateRandomDSTZone();

        ZoneOffsetTransition zoneOffsetTransition = timeZone.getRules().nextTransition(Instant.now());

        ZonedDateTime midnightBefore = zoneOffsetTransition.getDateTimeBefore().atZone(timeZone).minusDays(2).withHour(0).withMinute(0);
        ZonedDateTime midnightAfter = zoneOffsetTransition.getDateTimeAfter().atZone(timeZone).plusDays(2).withHour(0).withMinute(0);

        long epochBefore = midnightBefore.toInstant().toEpochMilli();
        long epochAfter = midnightAfter.toInstant().toEpochMilli();

        Cron cron = new Cron("0 0 2 * * ?", getTimeZone(timeZone)); // Every day at 2:00

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

    private ZoneId generateRandomDSTZone() {
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

        logger.debug("Testing for timezone {} after {} iterations", timeZone, i);
        return timeZone;
    }

    public void testForGMTAdvanceTransitionTriggerTimeIsRoundedToAfterDiscontinuity() {
        ZoneId london = ZoneId.of("Europe/London");
        Cron cron = new Cron("0 30 1 * * ?", getTimeZone(london)); // Every day at 1:30

        Instant beforeTransition = Instant.parse("2025-03-30T00:00:00Z");
        long beforeTransitionEpoch = beforeTransition.toEpochMilli();

        long nextValidTimeAfter = cron.getNextValidTimeAfter(beforeTransitionEpoch);
        System.out.println("nextValidTimeAfter = " + nextValidTimeAfter);
        assertThat(nextValidTimeAfter, equalTo(Instant.parse("2025-03-30T01:00:00Z").toEpochMilli()));
    }

    public void testForGMTRetardTransitionTriggerSkipSecondExecution() {
        ZoneId london = ZoneId.of("Europe/London");
        Cron cron = new Cron("0 30 1 * * ?", getTimeZone(london)); // Every day at 01:30

        Instant beforeTransition = Instant.parse("2024-10-27T00:00:00Z");
        long beforeTransitionEpoch = beforeTransition.toEpochMilli();

        long firstValidTimeAfter = cron.getNextValidTimeAfter(beforeTransitionEpoch);
        System.out.println("nextValidTimeAfter = " + firstValidTimeAfter);
        assertThat(firstValidTimeAfter, equalTo(Instant.parse("2024-10-27T00:30:00Z").toEpochMilli()));

        long nextValidTimeAfter = cron.getNextValidTimeAfter(firstValidTimeAfter);
        System.out.println("nextValidTimeAfter = " + nextValidTimeAfter);
        assertThat(nextValidTimeAfter, equalTo(Instant.parse("2024-10-28T01:30:00Z").toEpochMilli()));
    }

    // This test checks that once per minute crons will be unaffected by a DST transition
    public void testDiscontinuityResolutionForNonHourCronInRandomTimezone() {
        var timezone = generateRandomDSTZone();

        var cron = new Cron("0 * * * * ?", getTimeZone(timezone)); // Once per minute

        Instant referenceTime = randomInstantBetween(Instant.now(), Instant.now().plus(1826, ChronoUnit.DAYS)); // ~5 years
        ZoneOffsetTransition transition1 = timezone.getRules().nextTransition(referenceTime);

        // Currently there are no known timezones with DST transitions shorter than 10 minutes but this guards against future changes
        if (Math.abs(transition1.getOffsetBefore().getTotalSeconds() - transition1.getOffsetAfter().getTotalSeconds()) < 600) {
            fail("Transition is not long enough to test");
        }

        testNonHourCronTransition(transition1, cron);

        var transition2 = timezone.getRules().nextTransition(transition1.getInstant().plus(1, ChronoUnit.DAYS));

        testNonHourCronTransition(transition2, cron);

    }

    private static void testNonHourCronTransition(ZoneOffsetTransition transition, Cron cron) {
        Instant insideTransition;
        if (transition.isGap()) {
            insideTransition = transition.getInstant().plus(10, ChronoUnit.MINUTES);
            Instant nextTrigger = Instant.ofEpochMilli(cron.getNextValidTimeAfter(insideTransition.toEpochMilli()));
            assertThat(nextTrigger, equalTo(insideTransition.plus(1, ChronoUnit.MINUTES)));
        } else {
            insideTransition = transition.getInstant().minus(10, ChronoUnit.MINUTES);
            Instant nextTrigger = Instant.ofEpochMilli(cron.getNextValidTimeAfter(insideTransition.toEpochMilli()));
            assertThat(nextTrigger, equalTo(insideTransition.plus(1, ChronoUnit.MINUTES)));

            insideTransition = insideTransition.plus(transition.getDuration());
            nextTrigger = Instant.ofEpochMilli(cron.getNextValidTimeAfter(insideTransition.toEpochMilli()));
            assertThat(nextTrigger, equalTo(insideTransition.plus(1, ChronoUnit.MINUTES)));
        }
    }

    // This test checks that once per minute crons will be unaffected by a DST transition
    public void testDiscontinuityResolutionForCronInRandomTimezone() {
        var timezone = generateRandomDSTZone();
        timezone = ZoneId.of("Europe/London");

        Instant referenceTime = randomInstantBetween(Instant.now(), Instant.now().plus(1826, ChronoUnit.DAYS)); // ~5 years
        ZoneOffsetTransition transition1 = timezone.getRules().nextTransition(referenceTime);

        // Currently there are no known timezones with DST transitions shorter than 10 minutes but this guards against future changes
        if (Math.abs(transition1.getOffsetBefore().getTotalSeconds() - transition1.getOffsetAfter().getTotalSeconds()) < 600) {
            fail("Transition is not long enough to test");
        }

        testHourCronTransition(transition1, timezone);

        var transition2 = timezone.getRules().nextTransition(transition1.getInstant().plus(1, ChronoUnit.DAYS));

        testHourCronTransition(transition2, timezone);
    }

    private static void testHourCronTransition(ZoneOffsetTransition transition, ZoneId timezone) {
        if (transition.isGap()) {
            LocalDateTime targetTime = transition.getDateTimeBefore().plusMinutes(10);

            var cron = new Cron("0 " + targetTime.getMinute() + " " + targetTime.getHour() + " * * ?", getTimeZone(timezone));

            long nextTrigger = cron.getNextValidTimeAfter(transition.getInstant().minus(10, ChronoUnit.MINUTES).toEpochMilli());

            assertThat(Instant.ofEpochMilli(nextTrigger), equalTo(transition.getInstant().plusSeconds(600)));
        } else {
            LocalDateTime targetTime = transition.getDateTimeAfter().plusMinutes(10);
            var cron = new Cron("0 " + targetTime.getMinute() + " " + targetTime.getHour() + " * * ?", getTimeZone(timezone));

            long firstTrigger = cron.
                getNextValidTimeAfter(
                transition.getInstant().minusSeconds(transition.getDuration().toSeconds()).minus(10, ChronoUnit.MINUTES).toEpochMilli()
            );

            assertThat(Instant.ofEpochMilli(firstTrigger), equalTo(transition.getInstant().plusSeconds(600)));

            var repeatTrigger = firstTrigger + (1000 * 10L);

            assertThat(repeatTrigger - firstTrigger, Matchers.greaterThan(1000 * 60 * 60 * 6L));
        }
    }

}
