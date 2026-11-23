/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessLookupBuilder;
import org.elasticsearch.painless.spi.PainlessTestScript;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;

import java.time.Month;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQuery;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;

/**
 * End-to-end tests for {@code @allocates} pre-checks: the annotated call's operands are replayed through the estimator
 * and its sanitized result charged before the call executes. Covers the built-in estimators, misbehaving-estimator
 * sanitization, and load-time failures for badly declared estimators.
 */
public class AllocationEstimatorTests extends AllocationTestCase {

    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        List<Whitelist> whitelists = new ArrayList<>(PAINLESS_BASE_WHITELIST);
        whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.allocation-estimator"));
        contexts.put(PainlessTestScript.CONTEXT, whitelists);
        return contexts;
    }

    // ---- Iterable augmentations ----

    public void testAsListCopiesOnlyWhenTheReceiverIsNotAList() {
        assertEquals(0L, AllocationEstimators.asListBytes(List.of()));
        assertEquals(40L, allocatedBytes("List l = new ArrayList(); l.asList(); return \"x\";"));
        assertEquals(
            AllocationEstimators.hashSetShellBytes() + AllocationEstimators.asListBytes(java.util.Set.of()),
            allocatedBytes("Set s = new HashSet(); s.asList(); return \"x\";")
        );
        assertEquals(0L, AllocationEstimators.asCollectionBytes(java.util.Set.of()));
    }

    public void testFindResultsAndGroupByChargedFromTheCollectionSize() {
        long lambda = AllocSizes.captureSize(1);
        assertEquals(
            40L + lambda + AllocationEstimators.findResultsBytes(null, List.of(), null),
            allocatedBytes("List l = new ArrayList(); l.findResults(x -> x); return \"x\";")
        );
        assertEquals(
            40L + lambda + AllocationEstimators.groupByBytes(null, List.of(), null),
            allocatedBytes("List l = new ArrayList(); l.groupBy(x -> x); return \"x\";")
        );
        assertThat(
            AllocationEstimators.groupByBytes(null, List.of("a", "b"), null),
            greaterThan(AllocationEstimators.groupByBytes(null, List.of("a"), null))
        );
    }

    public void testIterableJoinMatchesStringJoin() {
        assertEquals(AllocationEstimators.joinBytes(",", List.of()), AllocationEstimators.iterableJoinBytes(List.of(), ","));
        assertEquals(
            40L + AllocationEstimators.iterableJoinBytes(List.of(), ","),
            allocatedBytes("List l = new ArrayList(); l.join(','); return \"x\";")
        );
    }

    public void testSubstringChargedFromArguments() {
        long expected = AllocationEstimators.substringBytes("hello world", 0, 5);
        assertEquals(expected, allocatedBytes("String s = \"hello world\"; s.substring(0, 5); return \"x\";"));
    }

    public void testSubstringChargeVariesWithArguments() {
        // Same method, different arguments, different charge.
        long expected = AllocationEstimators.substringBytes("hello world", 0, 11);
        assertEquals(expected, allocatedBytes("String s = \"hello world\"; s.substring(0, 11); return \"x\";"));
    }

    public void testSubstringTripsLimit() {
        assertTripsLimit("String s = \"hello world\"; s.substring(0, 5); return \"x\";");
    }

    public void testConstructorChargedFromArguments() {
        // Inner new ArrayList() charges its constant 40; the outer copy constructor charges via the estimator.
        long expected = 40L + AllocationEstimators.arrayListCollectionBytes(new ArrayList<>());
        assertEquals(expected, allocatedBytes("new ArrayList(new ArrayList()); return \"x\";"));
    }

    public void testConstructorArgumentsEvaluatedExactlyOnce() {
        // The dynamic-constructor emission reorders evaluation (args before NEW); side effects must happen exactly once.
        PainlessTestScript script = compile(
            "List once(List l) { l.add(1); return l; } List src = new ArrayList(); new ArrayList(once(src)); return src.size();",
            "1mb"
        );
        assertEquals(1, script.execute());
    }

    public void testNegativeEstimateClampsToZero() {
        // A negative estimate (an estimator bug) charges nothing rather than crediting the running total.
        assertEquals(0L, allocatedBytes("AllocationEstimatorTestObject.negativeEstimated(); return \"x\";"));
    }

    public void testHugeEstimateTripsAnyLimit() {
        // Long.MAX_VALUE from an estimator must trip a roomy limit without overflowing the running total.
        PainlessTestScript script = compile("AllocationEstimatorTestObject.hugeEstimated(); return \"x\";", "1mb");
        Exception e = expectThrows(Exception.class, script::execute);
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t.getMessage() != null && t.getMessage().contains("allocation limit exceeded")) {
                return;
            }
        }
        throw new AssertionError("expected an allocation limit error, but got: " + e, e);
    }

    public void testAugmentedMethodEstimatorSeesReceiverFirst() {
        // The charge (receiver.length() * 100 + n) proves the estimator saw the receiver and the argument.
        assertEquals(3 * 100L + 7, allocatedBytes("String s = \"abc\"; s.augmentedEstimated(7); return \"x\";"));
    }

    public void testInheritedConstantChargedForStaticTypedCall() {
        // Statically-typed call to an unannotated implementation method; the constant annotation is on the interface and must be
        // charged via the inheritance walk (the direct-call counterpart of the def-dispatch inheritance test).
        assertEquals(
            56L,
            allocatedBytes("AllocationInheritanceObject x = new AllocationInheritanceObject(); x.inheritedConstant(); return \"y\";")
        );
    }

    public void testInheritedDynamicChargedForStaticTypedCall() {
        assertEquals(
            5 * 10L,
            allocatedBytes("AllocationInheritanceObject x = new AllocationInheritanceObject(); x.inheritedDynamic(5); return \"y\";")
        );
    }

    public void testConflictingAnnotationsAcrossWhitelistsRejected() {
        // Two allowlists annotating the same method differently fail via the existing duplicate-entry equivalence rule.
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            List<Whitelist> whitelists = new ArrayList<>(PAINLESS_BASE_WHITELIST);
            whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.allocation-estimator"));
            whitelists.add(
                WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.allocation-estimator-conflict")
            );
            PainlessLookupBuilder.buildFromWhitelists(whitelists, new HashMap<>(), new HashMap<>());
        });
        assertThat(e.getCause().getMessage(), containsString("cannot add methods with the same name and arity"));
    }

    public void testMissingEstimatorClassFailsAtLoadTime() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> loadTestWhitelist("org.elasticsearch.painless.allocation-estimator-missing-class")
        );
        assertThat(e.getCause().getMessage(), containsString("estimator class [org.elasticsearch.painless.DoesNotExist] not found"));
    }

    public void testMissingEstimatorMethodFailsAtLoadTime() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> loadTestWhitelist("org.elasticsearch.painless.allocation-estimator-missing-method")
        );
        assertThat(e.getCause().getMessage(), containsString("#nope"));
        assertThat(e.getCause().getMessage(), containsString("not found"));
    }

    public void testNonLongEstimatorFailsAtLoadTime() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> loadTestWhitelist("org.elasticsearch.painless.allocation-estimator-not-long")
        );
        assertThat(e.getCause().getMessage(), containsString("must be public static and return long"));
    }

    private static void loadTestWhitelist(String resource) {
        // Load alongside the base allowlist, as a plugin's would be, so common type names resolve.
        List<Whitelist> whitelists = new ArrayList<>(PAINLESS_BASE_WHITELIST);
        whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, resource));
        PainlessLookupBuilder.buildFromWhitelists(whitelists, new HashMap<>(), new HashMap<>());
    }

    // ---- java.time: flat values, composite chains, and text-sized formatter members. ----

    public void testFlatTimeValueCharged() {
        assertEquals(TimeAllocationEstimators.FLAT_VALUE_BYTES, allocatedBytes("Instant.ofEpochMilli(0); return 'x';"));
    }

    public void testFlatTimeValueChargedPerCall() {
        // One flat value per call.
        assertEquals(
            2 * TimeAllocationEstimators.FLAT_VALUE_BYTES,
            allocatedBytes("Instant i = Instant.ofEpochMilli(0); i.plusSeconds(1); return 'x';")
        );
    }

    public void testFlatTimeValueTripsLimit() {
        assertTripsLimit("Instant.ofEpochMilli(0); return 'x';");
    }

    public void testAtZoneChargesTheWholeChain() {
        // The Instant plus the whole zoned chain: ZonedDateTime, LocalDateTime, LocalDate and LocalTime.
        assertEquals(
            TimeAllocationEstimators.FLAT_VALUE_BYTES + TimeAllocationEstimators.ZONED_DATE_TIME_BYTES,
            allocatedBytes("Instant.ofEpochMilli(0).atZone(ZoneOffset.UTC); return 'x';")
        );
    }

    public void testAtOffsetChargesTheOffsetChain() {
        assertEquals(
            TimeAllocationEstimators.FLAT_VALUE_BYTES + TimeAllocationEstimators.OFFSET_DATE_TIME_BYTES,
            allocatedBytes("Instant.ofEpochMilli(0).atOffset(ZoneOffset.UTC); return 'x';")
        );
    }

    public void testZonedDateTimeArithmeticChargesTheChain() {
        assertEquals(
            TimeAllocationEstimators.FLAT_VALUE_BYTES + 2 * TimeAllocationEstimators.ZONED_DATE_TIME_BYTES,
            allocatedBytes("Instant.ofEpochMilli(0).atZone(ZoneOffset.UTC).plusDays(1); return 'x';")
        );
    }

    public void testZonedDateTimeChainTripsLimit() {
        assertTripsLimit("Instant.ofEpochMilli(0).atZone(ZoneOffset.UTC); return 'x';");
    }

    public void testZonedDateTimeParseChargedFromText() {
        String text = "2020-01-01T00:00:00Z";
        assertEquals(
            TimeAllocationEstimators.parseZonedDateTimeBytes(text),
            allocatedBytes("ZonedDateTime.parse('" + text + "'); return 'x';")
        );
    }

    public void testInstantParseChargedFromText() {
        String text = "2020-01-01T00:00:00Z";
        assertEquals(TimeAllocationEstimators.parseFlatValueBytes(text), allocatedBytes("Instant.parse('" + text + "'); return 'x';"));
    }

    public void testOfPatternChargeGrowsWithThePattern() {
        long shortPattern = allocatedBytes("DateTimeFormatter.ofPattern('yyyy'); return 'x';");
        long longPattern = allocatedBytes("DateTimeFormatter.ofPattern('yyyy-MM-dd HH:mm:ss.SSS'); return 'x';");
        assertEquals(TimeAllocationEstimators.ofPatternBytes("yyyy"), shortPattern);
        assertEquals(TimeAllocationEstimators.ofPatternBytes("yyyy-MM-dd HH:mm:ss.SSS"), longPattern);
        assertTrue("a longer pattern must cost more", longPattern > shortPattern);
    }

    public void testFormatChargedAsABoundedString() {
        long expected = TimeAllocationEstimators.ofPatternBytes("yyyy") + TimeAllocationEstimators.formatBytes(
            (DateTimeFormatter) null,
            (TemporalAccessor) null
        ) + TimeAllocationEstimators.FLAT_VALUE_BYTES + TimeAllocationEstimators.ZONED_DATE_TIME_BYTES;
        assertEquals(
            expected,
            allocatedBytes(
                "DateTimeFormatter f = DateTimeFormatter.ofPattern('yyyy');"
                    + "f.format(Instant.ofEpochMilli(0).atZone(ZoneOffset.UTC)); return 'x';"
            )
        );
    }

    public void testFormatterParseChargedFromText() {
        String text = "2020";
        long expected = TimeAllocationEstimators.ofPatternBytes("yyyy") + TimeAllocationEstimators.formatterParseBytes(null, text);
        assertEquals(expected, allocatedBytes("DateTimeFormatter.ofPattern('yyyy').parse('" + text + "'); return 'x';"));
    }

    public void testDeclinedTimeMembersChargeNothing() {
        // Fields and getters that return an existing object are not annotated, so they cost nothing.
        assertEquals(0L, allocatedBytes("ZoneOffset z = ZoneOffset.UTC; Instant e = Instant.EPOCH; return 'x';"));
        assertEquals(
            TimeAllocationEstimators.FLAT_VALUE_BYTES + TimeAllocationEstimators.ZONED_DATE_TIME_BYTES,
            allocatedBytes("Instant.ofEpochMilli(0).atZone(ZoneOffset.UTC).toLocalDate(); return 'x';")
        );
    }

    public void testZoneIdOfChargedAsTwoReferences() {
        // ZoneId.of builds a region object over cached rules each call; ZoneOffset.UTC is a constant and costs nothing.
        assertEquals(TimeAllocationEstimators.TWO_REFERENCE_BYTES, allocatedBytes("ZoneId.of('UTC'); return 'x';"));
        assertEquals(0, allocatedBytes("ZoneOffset.UTC.normalized(); return 'x';"));
    }

    public void testAvailableZoneIdsCharged() {
        assertEquals(TimeAllocationEstimators.availableZoneIdsBytes(), allocatedBytes("ZoneId.getAvailableZoneIds(); return 'x';"));
    }

    public void testDisplayNameChargedAsAFormatterAndAString() {
        long expected = TimeAllocationEstimators.displayNameBytes((Month) null, null, null);
        assertEquals(expected, allocatedBytes("Month.JANUARY.getDisplayName(TextStyle.FULL, Locale.ROOT); return 'x';"));
        assertTrue(expected > TimeAllocationEstimators.localizedFormatterBytes(null));
    }

    public void testLocalizedFormatterAndFormatAdapterCharged() {
        assertEquals(
            TimeAllocationEstimators.localizedFormatterBytes(null),
            allocatedBytes("DateTimeFormatter.ofLocalizedDate(FormatStyle.SHORT); return 'x';")
        );
        assertEquals(TimeAllocationEstimators.TWO_REFERENCE_BYTES, allocatedBytes("DateTimeFormatter.ISO_DATE.toFormat(); return 'x';"));
        // The combined pattern is joined into a new string on every call.
        assertEquals(
            TimeAllocationEstimators.localizedPatternBytes(null, null, null, null),
            allocatedBytes(
                "DateTimeFormatterBuilder.getLocalizedDateTimePattern(FormatStyle.SHORT, FormatStyle.SHORT, "
                    + "IsoChronology.INSTANCE, Locale.ROOT);"
                    + " return 'x';"
            )
        );
    }

    public void testParseBestChargesTheSameAsParseWithAQuery() {
        assertEquals(
            TimeAllocationEstimators.formatterParseBytes(null, "2020-01-01", (TemporalQuery<?>) null),
            TimeAllocationEstimators.formatterParseBytes(null, "2020-01-01", (TemporalQuery<?>[]) null)
        );
    }

    public void testRangeChargedAsAValueRange() {
        String date = "LocalDate d = LocalDate.of(2020, 1, 1); ";
        long base = allocatedBytes(date + "return 'x';");
        assertEquals(
            TimeAllocationEstimators.VALUE_RANGE_BYTES,
            allocatedBytes(date + "d.range(ChronoField.DAY_OF_MONTH); return 'x';") - base
        );
    }

    public void testChronoClassesCharged() {
        // Non-ISO dates have their own entries, which shadow the chrono interface ones.
        assertEquals(
            2 * TimeAllocationEstimators.CHRONO_DATE_BYTES,
            allocatedBytes("JapaneseDate.of(2000, 1, 1).plus(1, ChronoUnit.DAYS); return 'x';")
        );
        assertEquals(TimeAllocationEstimators.FLAT_VALUE_BYTES, allocatedBytes("IsoChronology.INSTANCE.date(2020, 1, 1); return 'x';"));
        // Through the interface the chronology is unknown, so the bigger non-ISO shapes are charged.
        assertEquals(
            TimeAllocationEstimators.CHRONO_DATE_BYTES + TimeAllocationEstimators.CHRONO_ZONED_DATE_TIME_BYTES,
            allocatedBytes(
                "Chronology c = IsoChronology.INSTANCE; c.date(2020, 1, 1); c.zonedDateTime(Instant.EPOCH, ZoneOffset.UTC); return 'x';"
            )
        );
    }

    public void testSortedViewsCharged() {
        // The keys are boxed up front so only the views are measured.
        String map = "TreeMap m = new TreeMap(); def lo = 1; def hi = 2; ";
        long base = allocatedBytes(map + "return 'x';");
        long expected = AllocationEstimators.subMapBytes((SortedMap<?, ?>) null, null) + AllocationEstimators.mapViewBytes(
            (NavigableMap<?, ?>) null
        ) + AllocationEstimators.subMapBytes((NavigableMap<?, ?>) null, null, true, null, true);
        assertEquals(expected, allocatedBytes(map + "m.headMap(lo); m.descendingMap(); m.subMap(lo, true, hi, true); return 'x';") - base);
    }

    public void testLocalDateArithmeticChargedFlat() {
        assertEquals(2 * TimeAllocationEstimators.FLAT_VALUE_BYTES, allocatedBytes("LocalDate.of(2020, 1, 1).plusDays(1); return 'x';"));
    }

    public void testLocalDateTimeChainCharged() {
        // The date, the date-time chain it starts, and the zoned chain on top.
        assertEquals(
            TimeAllocationEstimators.FLAT_VALUE_BYTES + TimeAllocationEstimators.LOCAL_DATE_TIME_BYTES
                + TimeAllocationEstimators.ZONED_DATE_TIME_BYTES,
            allocatedBytes("LocalDate.of(2020, 1, 1).atStartOfDay().atZone(ZoneOffset.UTC); return 'x';")
        );
    }

    public void testOffsetTimeChainCharged() {
        assertEquals(
            TimeAllocationEstimators.FLAT_VALUE_BYTES + TimeAllocationEstimators.OFFSET_TIME_BYTES,
            allocatedBytes("LocalTime.of(1, 2).atOffset(ZoneOffset.UTC); return 'x';")
        );
    }

    public void testDurationCharged() {
        assertEquals(
            TimeAllocationEstimators.FLAT_VALUE_BYTES,
            allocatedBytes("Duration.between(Instant.EPOCH, Instant.EPOCH); return 'x';")
        );
        assertEquals(TimeAllocationEstimators.parseFlatValueBytes("PT1S"), allocatedBytes("Duration.parse('PT1S'); return 'x';"));
    }

    public void testLocalDateParseWithFormatterChargedFromText() {
        String text = "2020-01-01";
        assertEquals(
            TimeAllocationEstimators.parseFlatValueBytes(text),
            allocatedBytes("LocalDate.parse('" + text + "', DateTimeFormatter.ISO_LOCAL_DATE); return 'x';")
        );
    }

    public void testOffsetDateTimeFormatCharged() {
        long expected = TimeAllocationEstimators.OFFSET_DATE_TIME_BYTES + TimeAllocationEstimators.formatBytes(
            (DateTimeFormatter) null,
            (TemporalAccessor) null
        );
        assertEquals(
            expected,
            allocatedBytes("OffsetDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE); return 'x';")
        );
    }

    public void testTemporalInterfaceCallChargesTheBiggestChain() {
        // Through the interface the result type is unknown, so the zoned chain is charged even for a LocalDate.
        assertEquals(
            TimeAllocationEstimators.FLAT_VALUE_BYTES + TimeAllocationEstimators.ZONED_DATE_TIME_BYTES,
            allocatedBytes("Temporal t = LocalDate.of(2020, 1, 1); t.plus(1, ChronoUnit.DAYS); return 'x';")
        );
    }

    public void testValueRangeCharged() {
        assertEquals(TimeAllocationEstimators.VALUE_RANGE_BYTES, allocatedBytes("ValueRange.of(1, 2); return 'x';"));
    }

    public void testFormatterBuilderCharged() {
        long expected = TimeAllocationEstimators.formatterBuilderBytes() + TimeAllocationEstimators.patternStepBytes(null, "yyyy")
            + TimeAllocationEstimators.toFormatterBytes(null);
        assertEquals(expected, allocatedBytes("new DateTimeFormatterBuilder().appendPattern('yyyy').toFormatter(); return 'x';"));
    }

    public void testFormatterCopyCharged() {
        assertEquals(
            TimeAllocationEstimators.FORMATTER_BYTES,
            allocatedBytes("DateTimeFormatter.ISO_DATE.withZone(ZoneOffset.UTC); return 'x';")
        );
    }

    public void testDateGettersChargeNothing() {
        // Getters return a part of an existing value.
        assertEquals(
            TimeAllocationEstimators.LOCAL_DATE_TIME_BYTES,
            allocatedBytes("LocalDateTime d = LocalDateTime.of(2020, 1, 1, 0, 0); d.toLocalDate(); d.getMonth(); d.getYear(); return 'x';")
        );
    }

    public void testMapViewsCharged() {
        long base = allocatedBytes("Map m = new HashMap(); return 'x';");
        long views = allocatedBytes("Map m = new HashMap(); m.keySet(); m.values(); m.entrySet(); return 'x';");
        assertEquals(3 * AllocationEstimators.mapViewBytes((Map<?, ?>) null), views - base);
    }

    public void testStreamIteratorCharged() {
        long base = allocatedBytes("[1, 2].stream(); return 'x';");
        long withIterator = allocatedBytes("[1, 2].stream().iterator(); return 'x';");
        assertEquals(AllocSizes.ITERATOR_BYTES, withIterator - base);
    }

    public void testPrimitiveStreamIteratorCharged() {
        // IntStream, LongStream and DoubleStream redeclare iterator(), so each needs its own entry.
        long base = allocatedBytes("IntStream.range(0, 2); LongStream.range(0, 2); DoubleStream.of(new double[] {1.0}); return 'x';");
        long withIterators = allocatedBytes(
            "IntStream.range(0, 2).iterator(); LongStream.range(0, 2).iterator(); "
                + "DoubleStream.of(new double[] {1.0}).iterator(); return 'x';"
        );
        assertEquals(3 * AllocSizes.ITERATOR_BYTES, withIterators - base);
    }

    public void testChronoInterfaceEntriesCharged() {
        // format and toInstant are declared only on the chrono interfaces, so the charge must come from those entries.
        long format = TimeAllocationEstimators.formatBytes((DateTimeFormatter) null, (TemporalAccessor) null);
        String zoned = "ZonedDateTime z = Instant.ofEpochMilli(0).atZone(ZoneOffset.UTC); ";
        long zonedBase = allocatedBytes(zoned + "return 'x';");

        assertEquals(TimeAllocationEstimators.FLAT_VALUE_BYTES, allocatedBytes(zoned + "z.toInstant(); return 'x';") - zonedBase);
        assertEquals(format, allocatedBytes(zoned + "z.format(DateTimeFormatter.ISO_INSTANT); return 'x';") - zonedBase);
        assertEquals(
            TimeAllocationEstimators.ZONED_DATE_TIME_BYTES,
            allocatedBytes(zoned + "z.withZoneSameInstant(ZoneOffset.UTC); return 'x';") - zonedBase
        );

        String local = "LocalDateTime d = LocalDateTime.of(2020, 1, 1, 0, 0); ";
        long localBase = allocatedBytes(local + "return 'x';");
        assertEquals(
            TimeAllocationEstimators.FLAT_VALUE_BYTES,
            allocatedBytes(local + "d.toInstant(ZoneOffset.UTC); return 'x';") - localBase
        );
        assertEquals(format, allocatedBytes(local + "d.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME); return 'x';") - localBase);

        String date = "LocalDate d = LocalDate.of(2020, 1, 1); ";
        long dateBase = allocatedBytes(date + "return 'x';");
        assertEquals(format, allocatedBytes(date + "d.format(DateTimeFormatter.ISO_LOCAL_DATE); return 'x';") - dateBase);
    }

    public void testChronoInterfaceEntriesChargedThroughDefDispatch() {
        String zoned = "def z = Instant.ofEpochMilli(0).atZone(ZoneOffset.UTC); ";
        long zonedBase = allocatedBytes(zoned + "return 'x';");
        assertEquals(TimeAllocationEstimators.FLAT_VALUE_BYTES, allocatedBytes(zoned + "z.toInstant(); return 'x';") - zonedBase);
    }

    public void testFormatterResolverFieldsCopyCharged() {
        // withResolverFields copies the set, so the charge grows with it.
        String fields =
            "Set s = new HashSet(); s.add(ChronoField.YEAR); s.add(ChronoField.MONTH_OF_YEAR); s.add(ChronoField.DAY_OF_MONTH); ";
        long base = allocatedBytes(fields + "return 'x';");
        long withCopy = allocatedBytes(fields + "DateTimeFormatter.ISO_DATE.withResolverFields(s); return 'x';");
        Set<?> three = Set.of(ChronoField.YEAR, ChronoField.MONTH_OF_YEAR, ChronoField.DAY_OF_MONTH);
        assertEquals(TimeAllocationEstimators.formatterCopyBytes(null, three), withCopy - base);
        assertTrue(TimeAllocationEstimators.formatterCopyBytes(null, three) > TimeAllocationEstimators.formatterCopyBytes(null, Set.of()));
    }

    public void testAppendZoneTextCopyCharged() {
        // appendZoneText copies a non-empty set of preferred zones.
        String zones = "Set s = new HashSet(); s.add(ZoneOffset.UTC); s.add(ZoneId.of('Europe/Paris')); "
            + "DateTimeFormatterBuilder b = new DateTimeFormatterBuilder(); ";
        long base = allocatedBytes(zones + "return 'x';");
        long withStep = allocatedBytes(zones + "b.appendZoneText(TextStyle.FULL, s); return 'x';");
        Set<?> two = Set.of(ZoneOffset.UTC, ZoneId.of("Europe/Paris"));
        assertEquals(TimeAllocationEstimators.builderStepBytes(null, null, two), withStep - base);
        assertTrue(
            TimeAllocationEstimators.builderStepBytes(null, null, two) > TimeAllocationEstimators.builderStepBytes(null, null, Set.of())
        );
    }

    public void testTimeEstimatorChargedThroughDefDispatch() {
        // A def call must charge the same as a typed call.
        assertEquals(
            TimeAllocationEstimators.FLAT_VALUE_BYTES + TimeAllocationEstimators.ZONED_DATE_TIME_BYTES,
            allocatedBytes("def i = Instant.ofEpochMilli(0); i.atZone(ZoneOffset.UTC); return 'x';")
        );
    }

    public void testEstimatorInNonAllowlistedClassCharged() {
        // The estimator class is only named by the annotation, never allowlisted, like the x-pack ones.
        assertEquals(5 * 8L, allocatedBytes("new AllocationEstimatorTestObject().externallyEstimated(5); return \"x\";"));
    }

    public void testEstimatorClassIsLinkableButNotVisibleToScripts() {
        // A plugin's estimator is not in the generated script's loader, so it must be registered or the script fails to
        // link. A unit test cannot reproduce that: one flat classpath means the parent loader finds everything. So assert
        // the registration instead.
        PainlessLookup lookup = PainlessLookupBuilder.buildFromWhitelists(
            scriptContexts().get(PainlessTestScript.CONTEXT),
            new HashMap<>(),
            new HashMap<>()
        );
        String estimatorClass = AllocationExternalEstimators.class.getName();

        assertNotNull("estimator class must be resolvable by the generated script's loader", lookup.javaClassNameToClass(estimatorClass));

        // ... and gains no script-visible surface by being registered.
        assertFalse(lookup.isValidCanonicalClassName(estimatorClass));
        assertFalse(lookup.isValidCanonicalClassName("AllocationExternalEstimators"));
        assertNull(lookup.canonicalTypeNameToType(estimatorClass));
        assertNull("no PainlessClass means no callable members", lookup.lookupPainlessClass(AllocationExternalEstimators.class));
        assertFalse(lookup.getClasses().stream().anyMatch(c -> c == AllocationExternalEstimators.class));
    }

    // ---- getter shortcuts ----

    public void testGetterShortcutCharged() {
        // o.estimatedValue resolves to getEstimatedValue(), so the shorthand read charges the same as the call.
        String call = "AllocationEstimatorTestObject o = new AllocationEstimatorTestObject(); int v = o.getEstimatedValue(); return \"x\";";
        String shortcut = "AllocationEstimatorTestObject o = new AllocationEstimatorTestObject(); int v = o.estimatedValue; return \"x\";";
        assertEquals(72L, allocatedBytes(call));
        assertEquals(72L, allocatedBytes(shortcut));
    }

    public void testGetterShortcutTripsLimit() {
        assertTripsLimit("AllocationEstimatorTestObject o = new AllocationEstimatorTestObject(); int v = o.estimatedValue; return \"x\";");
    }
}
