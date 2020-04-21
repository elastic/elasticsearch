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

package org.elasticsearch.common;

import org.elasticsearch.common.LocalTimeOffset.FixedLookup;
import org.elasticsearch.common.LocalTimeOffset.Gap;
import org.elasticsearch.common.LocalTimeOffset.Overlap;
import org.elasticsearch.common.LocalTimeOffset.PreBuiltOffsetLookup;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.LongFunction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class LocalTimeOffsetTests extends ESTestCase {
    // NOCOMMIT test for checking for lookupFixedOffset
    public void testUtc() {
        LongFunction<LocalTimeOffset> lookup = unbound(ZoneId.of("UTC"));
        assertThat(lookup, instanceOf(FixedLookup.class));
        assertThat(lookup.apply(randomLong()).millis(), equalTo(0L));
    }

    public void testFixedOffset() {
        ZoneOffset offset = ZoneOffset.ofHours(between(-18, 18));
        LongFunction<LocalTimeOffset> lookup = unbound(ZoneId.ofOffset("UTC", offset));
        assertThat(lookup, instanceOf(FixedLookup.class));
        long t = randomValueOtherThan(Long.MIN_VALUE, ESTestCase::randomLong);
        LocalTimeOffset transition = lookup.apply(t);
        assertThat(transition.millis(), equalTo(offset.getTotalSeconds() * 1000L));
    }

    public void testPreBuiltTransitionsBeforeRules() {
        ZoneId zone = ZoneId.of("America/New_York");
        long min = time("1980-01-01", zone);
        long max = time("1981-01-01", zone) - 1;
        assertThat(Instant.ofEpochMilli(min), lessThan(lastTransitionIn(zone).getInstant()));
        LongFunction<LocalTimeOffset> lookup = LocalTimeOffset.lookup(zone, min, max);
        assertThat(lookup, instanceOf(PreBuiltOffsetLookup.class));
        assertThat(((PreBuiltOffsetLookup) lookup).transitions(), hasSize(3));
        assertThat(lookup.apply(min).millis(), equalTo(TimeUnit.HOURS.toMillis(-5L)));
        assertThat(lookup.apply(time("1980-06-01", zone)).millis(), equalTo(TimeUnit.HOURS.toMillis(-4L)));
        assertThat(lookup.apply(max).millis(), equalTo(TimeUnit.HOURS.toMillis(-5L)));
    }

    public void testPreBuiltTransitionsWithTransitionsAndRules() {
        ZoneId zone = ZoneId.of("America/New_York");
        long min = time("1980-01-01", zone);
        long max = time("2021-01-01", zone) - 1;
        assertThat(Instant.ofEpochMilli(min), lessThan(lastTransitionIn(zone).getInstant()));
        LongFunction<LocalTimeOffset> lookup = LocalTimeOffset.lookup(zone, min, max);
        assertThat(lookup, instanceOf(PreBuiltOffsetLookup.class));
        assertThat(((PreBuiltOffsetLookup) lookup).transitions(), hasSize(83));

        assertThat(lookup.apply(min).millis(), equalTo(TimeUnit.HOURS.toMillis(-5L)));
        assertThat(lookup.apply(time("2000-06-10", zone)).millis(), equalTo(TimeUnit.HOURS.toMillis(-4L)));
        assertThat(lookup.apply(max).millis(), equalTo(TimeUnit.HOURS.toMillis(-5L)));
    }

    public void testPreBuiltTransitionsAfterRules() {
        ZoneId zone = ZoneId.of("America/New_York");
        long min = time("2020-01-01", zone);
        long max = time("2021-01-01", zone) - 1;
        assertThat(Instant.ofEpochMilli(min), greaterThan(lastTransitionIn(zone).getInstant()));
        LongFunction<LocalTimeOffset> lookup = LocalTimeOffset.lookup(zone, min, max);
        assertThat(lookup, instanceOf(PreBuiltOffsetLookup.class));
        assertThat(((PreBuiltOffsetLookup) lookup).transitions(), hasSize(3));

        assertThat(lookup.apply(min).millis(), equalTo(TimeUnit.HOURS.toMillis(-5L)));
        assertThat(lookup.apply(time("2020-06-10", zone)).millis(), equalTo(TimeUnit.HOURS.toMillis(-4L)));
        assertThat(lookup.apply(max).millis(), equalTo(TimeUnit.HOURS.toMillis(-5L)));
    }

    public void testPreBuiltTransitionsSingleTimeBeforeRules() {
        ZoneId zone = ZoneId.of("America/New_York");
        long time = time("1980-01-01", zone);
        assertThat(Instant.ofEpochMilli(time), lessThan(lastTransitionIn(zone).getInstant()));
        LongFunction<LocalTimeOffset> lookup = LocalTimeOffset.lookup(zone, time, time);
        assertThat(lookup, instanceOf(PreBuiltOffsetLookup.class));
        // 2 because we include the transition before this time. Maybe we shouldn't?
        assertThat(((PreBuiltOffsetLookup) lookup).transitions(), hasSize(2));

        assertThat(lookup.apply(time).millis(), equalTo(TimeUnit.HOURS.toMillis(-5L)));
    }

    public void testPreBuiltTransitionsSingleTimeAfterRules() {
        ZoneId zone = ZoneId.of("America/New_York");
        long time = time("2020-01-01", zone);
        assertThat(Instant.ofEpochMilli(time), greaterThan(lastTransitionIn(zone).getInstant()));
        LongFunction<LocalTimeOffset> lookup = LocalTimeOffset.lookup(zone, time, time);
        assertThat(lookup, instanceOf(PreBuiltOffsetLookup.class));
        // 3 because we build all the transitions for the whole year
        assertThat(((PreBuiltOffsetLookup) lookup).transitions(), hasSize(3));

        assertThat(lookup.apply(time).millis(), equalTo(TimeUnit.HOURS.toMillis(-5L)));
    }

    public void testPreBuiltJustOneRuleApplies() {
        ZoneId zone = ZoneId.of("Atlantic/Azores");
        long time = time("2000-10-30T00:00:00", zone);
        LongFunction<LocalTimeOffset> lookup = LocalTimeOffset.lookup(zone, time, time);
        assertThat(lookup, instanceOf(PreBuiltOffsetLookup.class));
        assertThat(((PreBuiltOffsetLookup) lookup).transitions(), hasSize(2));

        assertThat(lookup.apply(time).millis(), equalTo(TimeUnit.HOURS.toMillis(-1L)));
    }

    public void testLastTransitionWithoutRules() {
        /*
         * Asia/Kathmandu turned their clocks 15 minutes forward at
         * 1986-01-01T00:00:00 local time and hasn't changed time since.
         */
        ZoneId zone = ZoneId.of("Asia/Kathmandu");
        long time = time("1986-01-01T00:00:00", zone);
        LongFunction<LocalTimeOffset> lookup = LocalTimeOffset.lookup(zone, time - 1, time);
        assertThat(lookup, instanceOf(PreBuiltOffsetLookup.class));
        assertThat(((PreBuiltOffsetLookup) lookup).transitions(), hasSize(2));

        assertThat(lookup.apply(time - 1).millis(), equalTo(TimeUnit.MINUTES.toMillis(330)));
        assertThat(lookup.apply(time).millis(), equalTo(TimeUnit.MINUTES.toMillis(345)));
    }

    public void testOverlap() {
        /*
         * Europe/Rome turn their clocks back an hour 1978 which is totally
         * normal, but they rolled back past midnight which is pretty rare and neat.
         */
        ZoneId tz = ZoneId.of("Europe/Rome");
        long overlapMillis = TimeUnit.HOURS.toMillis(1);
        long firstMidnight = utcTime("1978-09-30T22:00:00");
        long secondMidnight = utcTime("1978-09-30T23:00:00");
        long overlapEnds = utcTime("1978-10-01T0:00:00");
        LongFunction<LocalTimeOffset> lookup = LocalTimeOffset.lookup(tz, firstMidnight, overlapEnds);
        LocalTimeOffset secondMidnightOffset = lookup.apply(secondMidnight);
        long localSecondMidnight = secondMidnightOffset.utcToLocalTime(secondMidnight);
        LocalTimeOffset firstMidnightOffset = lookup.apply(firstMidnight);
        long localFirstMidnight = firstMidnightOffset.utcToLocalTime(firstMidnight);
        assertThat(localSecondMidnight - localFirstMidnight, equalTo(0L));
        assertThat(lookup.apply(overlapEnds), sameInstance(secondMidnightOffset));
        long localOverlapEnds = secondMidnightOffset.utcToLocalTime(overlapEnds);
        assertThat(localOverlapEnds - localSecondMidnight, equalTo(overlapMillis));

        long localOverlappingTime = randomLongBetween(localFirstMidnight, localOverlapEnds);

        assertThat(firstMidnightOffset.localToUtcInThisOffset(localFirstMidnight - 1), equalTo(firstMidnight - 1));
        assertThat(secondMidnightOffset.localToUtcInThisOffset(localFirstMidnight - 1), equalTo(secondMidnight - 1));
        assertThat(firstMidnightOffset.localToUtcInThisOffset(localFirstMidnight), equalTo(firstMidnight));
        assertThat(secondMidnightOffset.localToUtcInThisOffset(localFirstMidnight), equalTo(secondMidnight));
        assertThat(secondMidnightOffset.localToUtcInThisOffset(localOverlapEnds), equalTo(overlapEnds));
        assertThat(secondMidnightOffset.localToUtcInThisOffset(localOverlappingTime),
                equalTo(firstMidnightOffset.localToUtcInThisOffset(localOverlappingTime) + overlapMillis));
    
        assertThat(randomFrom(firstMidnightOffset, secondMidnightOffset).localToFirstUtc(localFirstMidnight - 1),
                equalTo(firstMidnight - 1));
        assertThat(randomFrom(firstMidnightOffset, secondMidnightOffset).localToFirstUtc(localFirstMidnight),
                equalTo(firstMidnight));
        assertThat(secondMidnightOffset.localToFirstUtc(localOverlapEnds), equalTo(overlapEnds));
        assertThat(secondMidnightOffset.localToFirstUtc(localOverlappingTime),
                equalTo(firstMidnightOffset.localToFirstUtc(localOverlappingTime)));

        long beforeOverlapValue = randomLong();
        assertThat(secondMidnightOffset.localToUtc(localFirstMidnight - 1, useValueForBeforeOverlap(beforeOverlapValue)),
                equalTo(beforeOverlapValue));
        long overlapValue = randomLong();
        assertThat(secondMidnightOffset.localToUtc(localFirstMidnight, useValueForOverlap(overlapValue)), equalTo(overlapValue));
        assertThat(secondMidnightOffset.localToUtc(localOverlapEnds, unusedStrategy()), equalTo(overlapEnds));
        assertThat(secondMidnightOffset.localToUtc(localOverlappingTime, useValueForOverlap(overlapValue)), equalTo(overlapValue));
    }

    public void testGap() {
        /*
         * Asia/Kathmandu turned their clocks 15 minutes forward at
         * 1986-01-01T00:00:00, creating a really "fun" gap.
         */
        ZoneId tz = ZoneId.of("Asia/Kathmandu");
        long gapLength = TimeUnit.MINUTES.toMillis(15);
        long transition = time("1986-01-01T00:00:00", tz);
        LongFunction<LocalTimeOffset> lookup = LocalTimeOffset.lookup(tz, transition - 1, transition);
        LocalTimeOffset gapOffset = lookup.apply(transition);
        long localAtTransition = gapOffset.utcToLocalTime(transition);
        LocalTimeOffset beforeGapOffset = lookup.apply(transition - 1);
        long localBeforeTransition = beforeGapOffset.utcToLocalTime(transition - 1);
        assertThat(localAtTransition - localBeforeTransition, equalTo(gapLength + 1));

        long localSkippedTime = randomLongBetween(localBeforeTransition, localAtTransition);

        assertThat(beforeGapOffset.localToUtcInThisOffset(localBeforeTransition), equalTo(transition - 1));
        assertThat(gapOffset.localToUtcInThisOffset(localBeforeTransition), equalTo(transition - 1 - gapLength));
        assertThat(gapOffset.localToUtcInThisOffset(localAtTransition), equalTo(transition));

        assertThat(randomFrom(gapOffset, beforeGapOffset).localToFirstUtc(localBeforeTransition), equalTo(transition - 1));
        assertThat(gapOffset.localToFirstUtc(localAtTransition), equalTo(transition));
        assertThat(gapOffset.localToFirstUtc(localSkippedTime), equalTo(transition));

        long beforeGapValue = randomLong();
        assertThat(gapOffset.localToUtc(localBeforeTransition, useValueForBeforeGap(beforeGapValue)), equalTo(beforeGapValue));
        assertThat(gapOffset.localToUtc(localAtTransition, unusedStrategy()), equalTo(transition));
        long gapValue = randomLong();
        assertThat(gapOffset.localToUtc(localSkippedTime, useValueForGap(gapValue)), equalTo(gapValue));
    }

    public void testNoLookup() {
        ZoneId zone = ZoneId.of("America/New_York");
        assertThat(unbound(zone), nullValue());
    }

    private LongFunction<LocalTimeOffset> unbound(ZoneId zone) {
        return LocalTimeOffset.lookup(zone, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    private static long utcTime(String time) {
        return DateFormatter.forPattern("date_optional_time").parseMillis(time);
    }

    private static long time(String time, ZoneId zone) {
        return DateFormatter.forPattern("date_optional_time").withZone(zone).parseMillis(time);
    }

    /**
     * The the last "fully defined" transitions in the provided {@linkplain ZoneId}.
     */
    private static ZoneOffsetTransition lastTransitionIn(ZoneId zone) {
        List<ZoneOffsetTransition> transitions = zone.getRules().getTransitions();
        return transitions.get(transitions.size() -1);
    }

    private static LocalTimeOffset.Strategy unusedStrategy() {
        return new LocalTimeOffset.Strategy() {
            @Override
            public long inGap(long localMillis, Gap gap) {
                fail("Shouldn't be called");
                return 0;
            }

            @Override
            public long beforeGap(long localMillis, Gap gap) {
                fail("Shouldn't be called");
                return 0;
            }

            @Override
            public long inOverlap(long localMillis, Overlap overlap) {
                fail("Shouldn't be called");
                return 0;
            }

            @Override
            public long beforeOverlap(long localMillis, Overlap overlap) {
                fail("Shouldn't be called");
                return 0;
            }
        };
    }

    private static LocalTimeOffset.Strategy useValueForGap(long gapValue) {
        return new LocalTimeOffset.Strategy() {
            @Override
            public long inGap(long localMillis, Gap gap) {
                return gapValue;
            }

            @Override
            public long beforeGap(long localMillis, Gap gap) {
                fail("Shouldn't be called");
                return 0;
            }

            @Override
            public long inOverlap(long localMillis, Overlap overlap) {
                fail("Shouldn't be called");
                return 0;
            }

            @Override
            public long beforeOverlap(long localMillis, Overlap overlap) {
                fail("Shouldn't be called");
                return 0;
            }
        };
    }

    private static LocalTimeOffset.Strategy useValueForBeforeGap(long beforeGapValue) {
        return new LocalTimeOffset.Strategy() {
            @Override
            public long inGap(long localMillis, Gap gap) {
                fail("Shouldn't be called");
                return 0;
            }

            @Override
            public long beforeGap(long localMillis, Gap gap) {
                return beforeGapValue;
            }

            @Override
            public long inOverlap(long localMillis, Overlap overlap) {
                fail("Shouldn't be called");
                return 0;
            }

            @Override
            public long beforeOverlap(long localMillis, Overlap overlap) {
                fail("Shouldn't be called");
                return 0;
            }
        };
    }

    private static LocalTimeOffset.Strategy useValueForOverlap(long overlapValue) {
        return new LocalTimeOffset.Strategy() {
            @Override
            public long inGap(long localMillis, Gap gap) {
                fail("Shouldn't be called");
                return 0;
            }

            @Override
            public long beforeGap(long localMillis, Gap gap) {
                fail("Shouldn't be called");
                return 0;
            }

            @Override
            public long inOverlap(long localMillis, Overlap overlap) {
                return overlapValue;
            }

            @Override
            public long beforeOverlap(long localMillis, Overlap overlap) {
                fail("Shouldn't be called");
                return 0;
            }
        };
    }


    private static LocalTimeOffset.Strategy useValueForBeforeOverlap(long beforeOverlapValue) {
        return new LocalTimeOffset.Strategy() {
            @Override
            public long inGap(long localMillis, Gap gap) {
                fail("Shouldn't be called");
                return 0;
            }

            @Override
            public long beforeGap(long localMillis, Gap gap) {
                fail("Shouldn't be called");
                return 0;
            }

            @Override
            public long inOverlap(long localMillis, Overlap overlap) {
                fail("Shouldn't be called");
                return 0;
            }

            @Override
            public long beforeOverlap(long localMillis, Overlap overlap) {
                return beforeOverlapValue;
            }
        };
    }
}
