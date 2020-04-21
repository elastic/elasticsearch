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
import java.util.Optional;
import java.util.function.LongFunction;

import static java.util.Collections.unmodifiableList;

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
 * A similarly terrifying thing happens again in the spring when my clocks skip
 * straight from 1:59am to 3am.
 * <p>
 * So there are two methods to convert from local time back to utc,
 * {@link #localToUtc(long, Strategy)} and {@link #localToFirstUtc(long)}.
 */
public abstract class LocalTimeOffset {
    /**
     * Lookup offsets for a provided zone. This <strong>can</strong> fail if
     * there are many transitions and the provided lookup would be very large.
     *
     * @return a lookup function of {@code null} if none could be built 
     */
    public static LongFunction<LocalTimeOffset> lookup(ZoneId zone, long minUtcMillis, long maxUtcMillis) {
        if (minUtcMillis > maxUtcMillis) {
            throw new IllegalArgumentException("[" + minUtcMillis + "] must be <= [" + maxUtcMillis + "]");
        }
        ZoneRules rules = zone.getRules();
        LocalTimeOffset fixed = checkForFixedZone(zone, rules);
        if (fixed != null) {
            return new FixedLookup(zone, fixed);
        }
        // NOCOMMIT it is really tempting to check if it is faster not to have the binary search when there are 2. That is common.
        // NOCOMMIT rework this
        return PreBuiltOffsetLookup.tryForZone(zone, rules, minUtcMillis, maxUtcMillis).orElseGet(() -> null);
    }

    /**
     * Lookup offsets without any known min or max time. This will generally
     * fail if the provided zone isn't fixed.
     *
     * @return a lookup function of {@code null} if none could be built 
     */
    public static LocalTimeOffset lookupFixedOffset(ZoneId zone) {
        return checkForFixedZone(zone, zone.getRules());
    }

    private final long millis;

    private LocalTimeOffset(long millis) {
        this.millis = millis;
    }

    /**
     * The number of milliseconds that the wall clock should be offset
     * from utc while the wall clock is inside this transition.
     */
    public final long millis() { // NOCOMMIT maybe millis shouldn't be public - just a transition in and transition out
        return millis;
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
     * {@link #localToFirstUtc(long)} or {@link #localToUtc(long, Strategy)}.
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
     * Map a local time that occurs during this offset or a previous offset
     * to a utc time biasing towards the earliest reasonable utc time. You
     * can use this if you've converted from utc to local, rounded down, and
     * then want to convert back to utc but you don't really care about how
     * to resulve "funny" edge cases. This resolves them <em>fairly</em>
     * sanely.
     * <p>
     * If a local time occurred twice then returns the earliest time. If a
     * local time never occurred then returns the utc time where the clock
     * jumped over that time. If a local time occurs in a previous transition
     * then uses <strong>that</strong> transition to convert it to utc.
     */
    public final long localToFirstUtc(long localMillis) { // TODO *first* isn't quite right because of gap handling. "sensible" might be?
        return localToUtc(localMillis, FIRST_STRAT);
    }
    private static final Strategy FIRST_STRAT = new Strategy() {
        @Override
        public long inGap(long localMillis, Gap gap) {
            return gap.startUtcMillis();
        }

        public long beforeGap(long localMillis, Gap gap) {
            return gap.previous().localToUtc(localMillis, FIRST_STRAT);
        };

        @Override
        public long inOverlap(long localMillis, Overlap overlap) {
            return overlap.previous().localToUtc(localMillis, FIRST_STRAT);
        }

        public long beforeOverlap(long localMillis, Overlap overlap) {
            return overlap.previous().localToUtc(localMillis, FIRST_STRAT);
        };
    };

    private static class NoPrevious extends LocalTimeOffset {
        public NoPrevious(long millis) {
            super(millis);
        }

        @Override
        public long localToUtc(long localMillis, Strategy strat) {
            return localToUtcInThisOffset(localMillis);
        }

        @Override
        public String toString() {
            return Long.toString(millis());
        }
    }

    public static abstract class Transition extends LocalTimeOffset {
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
        public String toString() {
            return "Gap of " + millis() + "@" + Instant.ofEpochMilli(startUtcMillis());
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
        public String toString() {
            return "Overlap of " + millis() + "@" + Instant.ofEpochMilli(startUtcMillis());
        }
    }

    static class FixedLookup implements LongFunction<LocalTimeOffset> {
        private final ZoneId zone;
        private final LocalTimeOffset fixed;

        private FixedLookup(ZoneId zone, LocalTimeOffset fixed) {
            this.zone = zone;
            this.fixed = fixed;
        }

        @Override
        public LocalTimeOffset apply(long value) {
            return fixed;
        }

        @Override
        public String toString() {
            return String.format(Locale.ROOT, "FixedOffsetLookup[for %s at %s]", zone, fixed);
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
     * Builds an array that can be {@link Arrays#binarySearch(long[], long)}ed
     * for the daylight savings time transitions.
     */
    static class PreBuiltOffsetLookup implements LongFunction<LocalTimeOffset> {
        private static final int MAX_TRANSITIONS = 5000; // NOCOMMIT what number goes here?

        static Optional<LongFunction<LocalTimeOffset>> tryForZone(ZoneId zone, ZoneRules rules, long minUtcMillis, long maxUtcMillis) {
            List<ZoneOffsetTransition> transitions = collectTransitions(zone, rules, minUtcMillis, maxUtcMillis);
            if (transitions == null) {
                // Too big
                return Optional.empty();
            }
            return Optional.of(new PreBuiltOffsetLookup(zone, rules, minUtcMillis, maxUtcMillis, transitions));
        }

        /**
         * Copy all transitions in {@link ZoneRules#getTransitions()} in the
         * range into a list.
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
                }
                transitions = buildTransitionsFromRules(transitions, zone, rules, minSecond, maxSecond);
                if (transitions != null && transitions.isEmpty()) {
                    /*
                     * If there aren't any rules and we haven't accumulated
                     * any transitions then we grab the last one we saw so we
                     * have some knowledge of the offset.
                     *
                     * This'd be a case where we probably can use a fixed lookup.
                     */
                    transitions.add(t);
                }
                return transitions;
            }
            transitions.add(t);
            while (itr.hasNext()) {
                t = itr.next();
                if (t.toEpochSecond() > maxSecond) {
                    // TODO if we only have a single rule and min and max and on the same side we could use a fixed offset lookup
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
                // TODO we can be a lot more careful in minYear == maxYear
            }
            if (minYear == maxYear) {
                if (transitions.isEmpty()) {
                    /*
                     * Make sure we have *some* transition to compare with.
                     * NOCOMMIT: this is actually a case where we can use a fixed offset!
                     */
                    transitions.add(lastTransitionFromMinYear);
                }
                return transitions;
            }

            /*
             * Now build transitions for all of the remaining years.
             */
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

        private final ZoneId zone;
        private final long minUtcMillis;
        private final long maxUtcMillis;
        private final LocalTimeOffset[] offsets;
        private final long[] transitionOutUtcMillis;

        private PreBuiltOffsetLookup(ZoneId zone, ZoneRules rules,
                long minUtcMillis, long maxUtcMillis, List<ZoneOffsetTransition> transitions) {
            this.zone = zone;
            this.minUtcMillis = minUtcMillis;
            this.maxUtcMillis = maxUtcMillis;
            this.offsets = new LocalTimeOffset[transitions.size() + 1];
            this.transitionOutUtcMillis = new long[transitions.size()];
            this.offsets[0] = new NoPrevious(transitions.get(0).getOffsetBefore().getTotalSeconds() * 1000);
            int i = 0;
            for (ZoneOffsetTransition t : transitions) {
                long utcStart = transitionOutUtcMillis[i] = t.toEpochSecond() * 1000;
                long offsetBeforeMillis = t.getOffsetBefore().getTotalSeconds() * 1000;
                long offsetAfterMillis = t.getOffsetAfter().getTotalSeconds() * 1000;
                LocalTimeOffset next;
                if (t.isGap()) {
                    long firstMissingLocalTime = utcStart + offsetBeforeMillis;
                    long firstLocalTimeAfterGap = utcStart + offsetAfterMillis;
                    next = new Gap(offsetAfterMillis, this.offsets[i], utcStart, firstMissingLocalTime, firstLocalTimeAfterGap);
                } else {
                    long firstOverlappingLocalTime = utcStart + offsetAfterMillis;
                    long firstNonOverlappingLocalTime = utcStart + offsetBeforeMillis;
                    next = new Overlap(offsetAfterMillis, this.offsets[i], utcStart,
                            firstOverlappingLocalTime, firstNonOverlappingLocalTime);
                }
                i++;
                this.offsets[i] = next;
            }
        }

        @Override
        public LocalTimeOffset apply(long utcMillis) {
            assert utcMillis >= minUtcMillis;
            assert utcMillis <= maxUtcMillis;
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
        public String toString() {
            return String.format(Locale.ROOT, "PreBuiltOffsetLookup[for %s between %s and %s]",
                    zone, Instant.ofEpochMilli(minUtcMillis), Instant.ofEpochMilli(maxUtcMillis));
        }

        List<LocalTimeOffset> transitions() {
            return unmodifiableList(Arrays.asList(offsets));
        }
    }
}
