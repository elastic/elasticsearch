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

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneOffsetTransitionRule;
import java.time.zone.ZoneRules;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

/**
 * Converts utc into local time and back again.
 * <p>
 * "Local time" is defined by some time zone, specifically and {@link ZoneId}.
 * At any point in time a particular time zone is at some offset from from
 * utc. So converting from utc is as simple as adding the offset.
 * <p>
 * Getting from local time back to utc is harder. Most local times happen once.
 * But some local times happen twice. And some don't happen at all. Take, for
 * example, the time in my house. Most days I don't touch my clocks and I'm a
 * constant offset from UTC. But once in the fall at 2am I roll my clock back.
 * So at 5am utc my clocks say 1am. Then at 6am utc my clocks say 1am AGAIN.
 * I do similarly terrifying things again in the spring when I skip my clocks
 * straight from 1:59am to 3am.
 * <p>
 * So there are two methods to convert from local time back to utc,
 * {@link #localToUtc(long, Strategy)} and {@link #localToUtcInThisOffset(long)}.
 */
public abstract class LocalTimeOffset {
    /**
     * Lookup offsets for a provided zone. This <strong>can</strong> fail if
     * there are many transitions and the provided lookup would be very large.
     *
     * @return a {@linkplain Lookup} or {@code null} if none could be built 
     */
    public static Lookup lookup(ZoneId zone, long minUtcMillis, long maxUtcMillis) {
        if (minUtcMillis > maxUtcMillis) {
            throw new IllegalArgumentException("[" + minUtcMillis + "] must be <= [" + maxUtcMillis + "]");
        }
        ZoneRules rules = zone.getRules();
        {
            LocalTimeOffset fixed = checkForFixedZone(zone, rules);
            if (fixed != null) {
                return new FixedLookup(zone, fixed);
            }
        }
        List<ZoneOffsetTransition> transitions = collectTransitions(zone, rules, minUtcMillis, maxUtcMillis);
        if (transitions == null) {
            // The range is too large for us to pre-build all the offsets
            return null;
        }
        if (transitions.size() < 3) {
            /*
             * Its actually quite common that there are *very* few transitions.
             * This case where there are only two transitions covers an entire
             * year of data! In any case, it is slightly faster to do the
             * "simpler" thing and compare the start times instead of perform
             * a binary search when there are so few offsets to look at.
             */
            return new LinkedListLookup(zone, minUtcMillis, maxUtcMillis, transitions);
        }
        return new TransitionArrayLookup(zone, minUtcMillis, maxUtcMillis, transitions);
    }

    /**
     * Lookup offsets without any known min or max time. This will generally
     * fail if the provided zone isn't fixed.
     *
     * @return a lookup function of {@code null} if none could be built 
     */
    public static LocalTimeOffset fixedOffset(ZoneId zone) {
        return checkForFixedZone(zone, zone.getRules());
    }

    private final long millis;

    private LocalTimeOffset(long millis) {
        this.millis = millis;
    }

    /**
     * Convert a time in utc into a the local time at this offset.
     */
    public final long utcToLocalTime(long utcMillis) {
        return utcMillis + millis;
    }

    /**
     * Convert a time in local millis to utc millis using <strong>this</strong> offset.
     * <p>
     * <strong>Important:</strong> Callers will rarely want to <strong>force</strong>
     * using this offset and are instead instead interested in picking an appropriate
     * offset for some local time that they have rounded down. In that case use
     * {@link #localToUtc(long, Strategy)}.
     */
    public final long localToUtcInThisOffset(long localMillis) {
        return localMillis - millis;
    }

    /**
     * Convert a local time that occurs during this offset or a previous
     * offset to utc, providing a strategy for how to resolve "funny" cases.
     * You can use this if you've converted from utc to local, rounded down,
     * and then want to convert back to utc and you need fine control over
     * how to handle the "funny" edges.
     * <p>
     * This will not help you if you must convert a local time that you've
     * rounded <strong>up</strong>. For that you are on your own. May God
     * have mercy on your soul.
     */
    public abstract long localToUtc(long localMillis, Strategy strat);
    public interface Strategy {
        /**
         * Handle a local time that never actually happened because a "gap"
         * jumped over it. This happens in many time zones when folks wind
         * their clocks forwards in the spring.
         *
         * @return the time in utc representing the local time
         */
        long inGap(long localMillis, Gap gap);
        /**
         * Handle a local time that happened before the start of a gap.
         *
         * @return the time in utc representing the local time
         */
        long beforeGap(long localMillis, Gap gap);
        /**
         * Handle a local time that happened twice because an "overlap"
         * jumped behind it. This happens in many time zones when folks wind
         * their clocks back in the fall.
         *
         * @return the time in utc representing the local time
         */
        long inOverlap(long localMillis, Overlap overlap);
        /**
         * Handle a local time that happened before the start of an overlap.
         *
         * @return the time in utc representing the local time
         */
        long beforeOverlap(long localMillis, Overlap overlap);
    }

    /**
     * Does this offset contain the provided time?
     */
    protected abstract boolean containsUtcMillis(long utcMillis);

    /**
     * Find the offset containing the provided time, first checking this
     * offset, then its previous offset, the than one's previous offset, etc.
     */
    protected abstract LocalTimeOffset offsetContaining(long utcMillis);

    @Override
    public String toString() {
        return toString(millis);
    }
    protected abstract String toString(long millis);

    /**
     * How to get instances of {@link LocalTimeOffset}.
     */
    public abstract static class Lookup {
        /**
         * Lookup the offset at the provided millis in utc.
         */
        public abstract LocalTimeOffset lookup(long utcMillis);

        /**
         * If the offset for a range is constant then return it, otherwise
         * return {@code null}.
         */
        public abstract LocalTimeOffset fixedInRange(long minUtcMillis, long maxUtcMillis);

        /**
         * The number of offsets in the lookup. Package private for testing.
         */
        abstract int size();
    }

    private static class NoPrevious extends LocalTimeOffset {
        NoPrevious(long millis) {
            super(millis);
        }

        @Override
        public long localToUtc(long localMillis, Strategy strat) {
            return localToUtcInThisOffset(localMillis);
        }

        @Override
        protected boolean containsUtcMillis(long utcMillis) {
            return true;
        }

        @Override
        protected LocalTimeOffset offsetContaining(long utcMillis) {
            /*
             * Since there isn't a previous offset this offset *must* contain
             * the provided time.
             */
            return this;
        }

        @Override
        protected String toString(long millis) {
            return Long.toString(millis);
        }
    }

    public abstract static class Transition extends LocalTimeOffset {
        private final LocalTimeOffset previous;
        private final long startUtcMillis;

        private Transition(long millis, LocalTimeOffset previous, long startUtcMillis) {
            super(millis);
            this.previous = previous;
            this.startUtcMillis = startUtcMillis;
        }

        /**
         * The offset before the this one.
         */
        public LocalTimeOffset previous() {
            return previous;
        }

        @Override
        protected final boolean containsUtcMillis(long utcMillis) {
            return utcMillis >= startUtcMillis;
        }

        @Override
        protected final LocalTimeOffset offsetContaining(long utcMillis) {
            if (containsUtcMillis(utcMillis)) {
                return this;
            }
            return previous.offsetContaining(utcMillis);
        }

        /**
         * The time that this offset started in milliseconds since epoch.
         */
        public long startUtcMillis() {
            return startUtcMillis;
        }
    }

    public static class Gap extends Transition {
        private final long firstMissingLocalTime;
        private final long firstLocalTimeAfterGap;

        private Gap(long millis, LocalTimeOffset previous, long startUtcMillis, long firstMissingLocalTime, long firstLocalTimeAfterGap) {
            super(millis, previous, startUtcMillis);
            this.firstMissingLocalTime = firstMissingLocalTime;
            this.firstLocalTimeAfterGap = firstLocalTimeAfterGap;
            assert firstMissingLocalTime < firstLocalTimeAfterGap;
        }

        @Override
        public long localToUtc(long localMillis, Strategy strat) {
            if (localMillis >= firstLocalTimeAfterGap) {
                return localToUtcInThisOffset(localMillis);
            }
            if (localMillis >= firstMissingLocalTime) {
                return strat.inGap(localMillis, this);
            }
            return strat.beforeGap(localMillis, this);
        }

        /**
         * The first time that is missing from the local time because of this gap.
         */
        public long firstMissingLocalTime() {
            return firstMissingLocalTime;
        }

        @Override
        protected String toString(long millis) {
            return "Gap of " + millis + "@" + Instant.ofEpochMilli(startUtcMillis());
        }
    }

    public static class Overlap extends Transition {
        private final long firstOverlappingLocalTime;
        private final long firstNonOverlappingLocalTime;

        private Overlap(long millis, LocalTimeOffset previous, long startUtcMillis,
                long firstOverlappingLocalTime, long firstNonOverlappingLocalTime) {
            super(millis, previous, startUtcMillis);
            this.firstOverlappingLocalTime = firstOverlappingLocalTime;
            this.firstNonOverlappingLocalTime = firstNonOverlappingLocalTime;
            assert firstOverlappingLocalTime < firstNonOverlappingLocalTime;
        }

        @Override
        public long localToUtc(long localMillis, Strategy strat) {
            if (localMillis >= firstNonOverlappingLocalTime) {
                return localToUtcInThisOffset(localMillis);
            }
            if (localMillis >= firstOverlappingLocalTime) {
                return strat.inOverlap(localMillis, this);
            }
            return strat.beforeOverlap(localMillis, this);
        }

        /**
         * The first local time after the overlap stops.
         */
        public long firstNonOverlappingLocalTime() {
            return firstNonOverlappingLocalTime;
        }

        /**
         * The first local time to be appear twice.
         */
        public long firstOverlappingLocalTime() {
            return firstOverlappingLocalTime;
        }

        @Override
        protected String toString(long millis) {
            return "Overlap of " + millis + "@" + Instant.ofEpochMilli(startUtcMillis());
        }
    }

    private static class FixedLookup extends Lookup {
        private final ZoneId zone;
        private final LocalTimeOffset fixed;

        private FixedLookup(ZoneId zone, LocalTimeOffset fixed) {
            this.zone = zone;
            this.fixed = fixed;
        }

        @Override
        public LocalTimeOffset lookup(long utcMillis) {
            return fixed;
        }

        @Override
        public LocalTimeOffset fixedInRange(long minUtcMillis, long maxUtcMillis) {
            return fixed;
        }

        @Override
        int size() {
            return 1;
        }

        @Override
        public String toString() {
            return String.format(Locale.ROOT, "FixedLookup[for %s at %s]", zone, fixed);
        }
    }

    /**
     * Looks up transitions by checking whether the date is after the start
     * of each transition. Simple so fast for small numbers of transitions.
     */
    private static class LinkedListLookup extends AbstractManyTransitionsLookup {
        private final LocalTimeOffset lastOffset;
        private final int size;

        LinkedListLookup(ZoneId zone, long minUtcMillis, long maxUtcMillis, List<ZoneOffsetTransition> transitions) {
            super(zone, minUtcMillis, maxUtcMillis);
            int size = 1;
            LocalTimeOffset last = buildNoPrevious(transitions.get(0));
            for (ZoneOffsetTransition t : transitions) {
                last = buildTransition(t, last);
                size++;
            }
            this.lastOffset = last;
            this.size = size;
        }

        @Override
        public LocalTimeOffset innerLookup(long utcMillis) {
            return lastOffset.offsetContaining(utcMillis);
        }

        @Override
        int size() {
            return size;
        }
    }

    /**
     * Builds an array that can be {@link Arrays#binarySearch(long[], long)}ed
     * for the daylight savings time transitions.
     */
    private static class TransitionArrayLookup extends AbstractManyTransitionsLookup {
        private final LocalTimeOffset[] offsets;
        private final long[] transitionOutUtcMillis;

        private TransitionArrayLookup(ZoneId zone, long minUtcMillis, long maxUtcMillis, List<ZoneOffsetTransition> transitions) {
            super(zone, minUtcMillis, maxUtcMillis);
            this.offsets = new LocalTimeOffset[transitions.size() + 1];
            this.transitionOutUtcMillis = new long[transitions.size()];
            this.offsets[0] = buildNoPrevious(transitions.get(0));
            int i = 0;
            for (ZoneOffsetTransition t : transitions) {
                Transition transition = buildTransition(t, this.offsets[i]);
                transitionOutUtcMillis[i] = transition.startUtcMillis();
                i++;
                this.offsets[i] = transition;
            }
        }

        @Override
        protected LocalTimeOffset innerLookup(long utcMillis) {
            int index = Arrays.binarySearch(transitionOutUtcMillis, utcMillis);
            if (index < 0) {
                /*
                 * We're mostly not going to find the exact offset. Instead we'll
                 * end up at the "insertion point" for the utcMillis. We have no
                 * plans to insert utcMillis in the array, but the offset that
                 * contains utcMillis happens to be "insertion point" - 1.
                 */
                index = -index - 1;
            } else {
                index++;
            }
            assert index < offsets.length : "binarySearch did something weird";
            return offsets[index];
        }

        @Override
        int size() {
            return offsets.length;
        }

        @Override
        public String toString() {
            return String.format(Locale.ROOT, "TransitionArrayLookup[for %s between %s and %s]",
                    zone, Instant.ofEpochMilli(minUtcMillis), Instant.ofEpochMilli(maxUtcMillis));
        }
    }

    private abstract static class AbstractManyTransitionsLookup extends Lookup {
        protected final ZoneId zone;
        protected final long minUtcMillis;
        protected final long maxUtcMillis;

        AbstractManyTransitionsLookup(ZoneId zone, long minUtcMillis, long maxUtcMillis) {
            this.zone = zone;
            this.minUtcMillis = minUtcMillis;
            this.maxUtcMillis = maxUtcMillis;
        }

        @Override
        public final LocalTimeOffset lookup(long utcMillis) {
            assert utcMillis >= minUtcMillis;
            assert utcMillis <= maxUtcMillis;
            return innerLookup(utcMillis);
        }

        protected abstract LocalTimeOffset innerLookup(long utcMillis);

        @Override
        public final LocalTimeOffset fixedInRange(long minUtcMillis, long maxUtcMillis) {
            LocalTimeOffset offset = lookup(maxUtcMillis);
            return offset.containsUtcMillis(minUtcMillis) ? offset : null;
        }

        protected static NoPrevious buildNoPrevious(ZoneOffsetTransition transition) {
            return new NoPrevious(transition.getOffsetBefore().getTotalSeconds() * 1000);
        }

        protected static Transition buildTransition(ZoneOffsetTransition transition, LocalTimeOffset previous) {
            long utcStart = transition.toEpochSecond() * 1000;
            long offsetBeforeMillis = transition.getOffsetBefore().getTotalSeconds() * 1000;
            long offsetAfterMillis = transition.getOffsetAfter().getTotalSeconds() * 1000;
            assert (false == previous instanceof Transition) || ((Transition) previous).startUtcMillis < utcStart :
                    "transition list out of order at [" + previous + "] and [" + transition + "]";
            assert previous.millis != offsetAfterMillis :
                    "transition list is has a duplicate at [" + previous + "] and [" + transition + "]";
            if (transition.isGap()) {
                long firstMissingLocalTime = utcStart + offsetBeforeMillis;
                long firstLocalTimeAfterGap = utcStart + offsetAfterMillis;
                return new Gap(offsetAfterMillis, previous, utcStart, firstMissingLocalTime, firstLocalTimeAfterGap);
            }
            long firstOverlappingLocalTime = utcStart + offsetAfterMillis;
            long firstNonOverlappingLocalTime = utcStart + offsetBeforeMillis;
            return new Overlap(offsetAfterMillis, previous, utcStart, firstOverlappingLocalTime, firstNonOverlappingLocalTime);
        }
    }

    private static LocalTimeOffset checkForFixedZone(ZoneId zone, ZoneRules rules) {
        if (false == rules.isFixedOffset()) {
            return null;
        }
        LocalTimeOffset fixedTransition = new NoPrevious(rules.getOffset(Instant.EPOCH).getTotalSeconds() * 1000);
        return fixedTransition;
    }

    /**
     * The maximum number of {@link ZoneOffsetTransition} to collect before
     * giving up because the date range will be "too big". I picked this number
     * fairly arbitrarily with the following goals:
     * <ol>
     * <li>Don't let {@code lookup(Long.MIN_VALUE, Long.MAX_VALUE)} consume all
     *     the memory in the JVM.
     * <li>It should be much larger than the number of offsets I'm bound to
     *     collect.
     * </ol>
     * {@code 5_000} collects about 2_500 years worth offsets which feels like
     * quite a few!
     */
    private static final int MAX_TRANSITIONS = 5000;

    /**
     * Collect transitions from the provided rules for the provided date range
     * into a list we can reason about. If we'd collect more than
     * {@link #MAX_TRANSITIONS} rules we'll abort, returning {@code null}
     * signaling that {@link LocalTimeOffset} is probably not the implementation
     * to use in this case.
     * <p>
     * {@link ZoneRules} gives us access to the local time transition database
     * with two method: {@link ZoneRules#getTransitions()} for "fully defined"
     * transitions and {@link ZoneRules#getTransitionRules()}. This first one
     * is a list of transitions and when the they happened. To get the full
     * picture of transitions you pick up from where that one leaves off using
     * the rules, which are basically factories that you give the year in local
     * time to build a transition for that year.
     * <p>
     * This method collects all of the {@link ZoneRules#getTransitions()} that
     * are relevant for the date range and, if our range extends past the last
     * transition, calls
     * {@link #buildTransitionsFromRules(List, ZoneId, ZoneRules, long, long)}
     * to build the remaining transitions to fully describe the range.
     */
    private static List<ZoneOffsetTransition> collectTransitions(ZoneId zone, ZoneRules rules, long minUtcMillis, long maxUtcMillis) {
        long minSecond = minUtcMillis / 1000;
        long maxSecond = maxUtcMillis / 1000;
        List<ZoneOffsetTransition> transitions = new ArrayList<>();
        ZoneOffsetTransition t = null;
        Iterator<ZoneOffsetTransition> itr = rules.getTransitions().iterator();
        // Skip all transitions that are before our start time
        while (itr.hasNext() && (t = itr.next()).toEpochSecond() < minSecond) {}
        if (false == itr.hasNext()) {
            if (minSecond < t.toEpochSecond() && t.toEpochSecond() < maxSecond) {
                transitions.add(t);
                /*
                 * Sometimes the rules duplicate the transitions. And
                 * duplicates confuse us. So we have to skip past them.
                 */
                minSecond = t.toEpochSecond() + 1;
            }
            transitions = buildTransitionsFromRules(transitions, zone, rules, minSecond, maxSecond);
            if (transitions != null && transitions.isEmpty()) {
                /*
                 * If there aren't any rules and we haven't accumulated
                 * any transitions then we grab the last one we saw so we
                 * have some knowledge of the offset.
                 */
                transitions.add(t);
            }
            return transitions;
        }
        transitions.add(t);
        while (itr.hasNext()) {
            t = itr.next();
            if (t.toEpochSecond() > maxSecond) {
                return transitions;
            }
            transitions.add(t);
            if (transitions.size() > MAX_TRANSITIONS) {
                return null;
            }
        }
        return buildTransitionsFromRules(transitions, zone, rules, t.toEpochSecond() + 1, maxSecond);
    }

    /**
     * Build transitions for every year in our range from the rules
     * stored in {@link ZoneRules#getTransitionRules()}.
     */
    private static List<ZoneOffsetTransition> buildTransitionsFromRules(List<ZoneOffsetTransition> transitions,
            ZoneId zone, ZoneRules rules, long minSecond, long maxSecond) {
        List<ZoneOffsetTransitionRule> transitionRules = rules.getTransitionRules();
        if (transitionRules.isEmpty()) {
            /*
             * Zones like Asia/Kathmandu don't have any rules so we don't
             * need to do any of this.
             */
            return transitions;
        }
        int minYear = LocalDate.ofInstant(Instant.ofEpochSecond(minSecond), zone).getYear();
        int maxYear = LocalDate.ofInstant(Instant.ofEpochSecond(maxSecond), zone).getYear();

        /*
         * Record only the rules from the current year that are greater
         * than the minSecond so we don't go back in time when coming from
         * a fixed transition.
         */
        ZoneOffsetTransition lastTransitionFromMinYear = null;
        for (ZoneOffsetTransitionRule rule : transitionRules) {
            lastTransitionFromMinYear = rule.createTransition(minYear);
            if (lastTransitionFromMinYear.toEpochSecond() < minSecond) {
                continue;
            }
            transitions.add(lastTransitionFromMinYear);
            if (transitions.size() > MAX_TRANSITIONS) {
                return null;
            }
        }
        if (minYear == maxYear) {
            if (transitions.isEmpty()) {
                // Make sure we have *some* transition to work with. 
                transitions.add(lastTransitionFromMinYear);
            }
            return transitions;
        }

        // Now build transitions for all of the remaining years.
        minYear++;
        if (transitions.size() + (maxYear - minYear) * transitionRules.size() > MAX_TRANSITIONS) {
            return null;
        }
        for (int year = minYear; year <= maxYear; year++) {
            for (ZoneOffsetTransitionRule rule : transitionRules) {
                transitions.add(rule.createTransition(year));
            }
        }
        return transitions;
    }
}
