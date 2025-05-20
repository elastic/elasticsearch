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

import static java.time.Instant.ofEpochMilli;
import static java.util.TimeZone.getTimeZone;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;

public class CronTimezoneTests extends ESTestCase {

    public void testForFixedOffsetCorrectlyCalculateNextRuntime() {
        Cron cron = new Cron("0 0 2 * * ?", ZoneOffset.of("+1"));
        long midnightUTC = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        long nextValidTimeAfter = cron.getNextValidTimeAfter(midnightUTC);
        assertThat(Instant.ofEpochMilli(nextValidTimeAfter), equalTo(Instant.parse("2020-01-01T01:00:00Z")));
    }

    public void testForLondonFixedDSTTransitionCheckCorrectSchedule() {
        ZoneId londonZone = getTimeZone("Europe/London").toZoneId();

        Cron cron = new Cron("0 0 2 * * ?", londonZone);
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

        logger.info("Testing for timezone {}", timeZone);

        ZoneOffsetTransition zoneOffsetTransition = timeZone.getRules().nextTransition(Instant.now());

        ZonedDateTime midnightBefore = zoneOffsetTransition.getDateTimeBefore().atZone(timeZone).minusDays(2).withHour(0).withMinute(0);
        ZonedDateTime midnightAfter = zoneOffsetTransition.getDateTimeAfter().atZone(timeZone).plusDays(2).withHour(0).withMinute(0);

        long epochBefore = midnightBefore.toInstant().toEpochMilli();
        long epochAfter = midnightAfter.toInstant().toEpochMilli();

        Cron cron = new Cron("0 0 2 * * ?", timeZone);

        long nextScheduleBefore = cron.getNextValidTimeAfter(epochBefore);
        long nextScheduleAfter = cron.getNextValidTimeAfter(epochAfter);

        assertThat(nextScheduleBefore - epochBefore, equalTo(2 * 60 * 60 * 1000L)); // 2 hours
        assertThat(nextScheduleAfter - epochAfter, equalTo(2 * 60 * 60 * 1000L)); // 2 hours

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

    public void testForGMTGapTransitionTriggerTimeIsAsIfTransitionHasntHappenedYet() {
        ZoneId london = ZoneId.of("Europe/London");
        Cron cron = new Cron("0 30 1 * * ?", london); // Every day at 1:30

        Instant beforeTransition = Instant.parse("2025-03-30T00:00:00Z");
        long beforeTransitionEpoch = beforeTransition.toEpochMilli();

        long nextValidTimeAfter = cron.getNextValidTimeAfter(beforeTransitionEpoch);
        assertThat(ofEpochMilli(nextValidTimeAfter), equalTo(Instant.parse("2025-03-30T01:30:00Z")));
    }

    public void testForGMTOverlapTransitionTriggerSkipSecondExecution() {
        ZoneId london = ZoneId.of("Europe/London");
        Cron cron = new Cron("0 30 1 * * ?", london); // Every day at 01:30

        Instant beforeTransition = Instant.parse("2024-10-27T00:00:00Z");
        long beforeTransitionEpoch = beforeTransition.toEpochMilli();

        long firstValidTimeAfter = cron.getNextValidTimeAfter(beforeTransitionEpoch);
        assertThat(ofEpochMilli(firstValidTimeAfter), equalTo(Instant.parse("2024-10-27T00:30:00Z")));

        long nextValidTimeAfter = cron.getNextValidTimeAfter(firstValidTimeAfter);
        assertThat(ofEpochMilli(nextValidTimeAfter), equalTo(Instant.parse("2024-10-28T01:30:00Z")));
    }

    // This test checks that once per minute crons will be unaffected by a DST transition
    public void testDiscontinuityResolutionForNonHourCronInRandomTimezone() {
        var timezone = generateRandomDSTZone();

        var cron = new Cron("0 * * * * ?", timezone); // Once per minute

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
            Instant nextTrigger = ofEpochMilli(cron.getNextValidTimeAfter(insideTransition.toEpochMilli()));
            assertThat(nextTrigger, equalTo(insideTransition.plus(1, ChronoUnit.MINUTES)));
        } else {
            insideTransition = transition.getInstant().minus(10, ChronoUnit.MINUTES);
            Instant nextTrigger = ofEpochMilli(cron.getNextValidTimeAfter(insideTransition.toEpochMilli()));
            assertThat(nextTrigger, equalTo(insideTransition.plus(1, ChronoUnit.MINUTES)));

            insideTransition = insideTransition.plus(transition.getDuration());
            nextTrigger = ofEpochMilli(cron.getNextValidTimeAfter(insideTransition.toEpochMilli()));
            assertThat(nextTrigger, equalTo(insideTransition.plus(1, ChronoUnit.MINUTES)));
        }
    }

    // This test checks that once per day crons will behave correctly during a DST transition
    public void testDiscontinuityResolutionForCronInRandomTimezone() {
        var timezone = generateRandomDSTZone();

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

            var cron = new Cron("0 " + targetTime.getMinute() + " " + targetTime.getHour() + " * * ?", timezone);

            long nextTrigger = cron.getNextValidTimeAfter(transition.getInstant().minus(10, ChronoUnit.MINUTES).toEpochMilli());

            assertThat(ofEpochMilli(nextTrigger), equalTo(transition.getInstant().plus(10, ChronoUnit.MINUTES)));
        } else {
            LocalDateTime targetTime = transition.getDateTimeAfter().plusMinutes(10);
            var cron = new Cron("0 " + targetTime.getMinute() + " " + targetTime.getHour() + " * * ?", timezone);

            long transitionLength = Math.abs(transition.getDuration().toSeconds());
            long firstTrigger = cron.getNextValidTimeAfter(
                transition.getInstant().minusSeconds(transitionLength).minus(10, ChronoUnit.MINUTES).toEpochMilli()
            );

            assertThat(
                ofEpochMilli(firstTrigger),
                equalTo(transition.getInstant().minusSeconds(transitionLength).plus(10, ChronoUnit.MINUTES))
            );

            var repeatTrigger = cron.getNextValidTimeAfter(firstTrigger + (1000 * 60L)); // 1 minute

            assertThat(repeatTrigger - firstTrigger, Matchers.greaterThan(24 * 60 * 60 * 1000L)); // 24 hours
        }
    }

}
