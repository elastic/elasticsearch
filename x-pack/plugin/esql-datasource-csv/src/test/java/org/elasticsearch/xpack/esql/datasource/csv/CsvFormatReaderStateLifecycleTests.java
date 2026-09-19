/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * The lifecycle gate over {@link CsvFormatReader}'s mutable state — the CSV/TSV twin of
 * {@code NdJsonFormatReaderStateLifecycleTests}. Invariant: every ordinary wither preserves the parent's counter
 * struct; {@link CsvFormatReader#withFreshCounters()} is the only method that mints a fresh one, and the operator
 * factory calls it once per {@code get(DriverContext)}. Isolation between operators is at
 * {@code withFreshCounters()}, not at the individual wither seam. The behavioural pins in
 * {@link CsvFormatReaderStatusSnapshotTests} guard each known wither; this class guards the ENUMERATION, so a
 * new wither or field fails the build until its lifecycle is decided.
 */
@SuppressForbidden(reason = "reflection over declared fields and withers is the point: an undeclared one must not slip past the gate")
public class CsvFormatReaderStateLifecycleTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("noop"))
        .build();

    /**
     * Instance fields carrying no cross-instance mutable state. {@code sharedCsvMapper} qualifies because every
     * copy builds its own in the constructor ({@code createMapper(options)}) and it is only configured there,
     * never reconfigured during reads.
     */
    private static final Set<String> IMMUTABLE_OR_CONFIG_FIELDS = Set.of(
        "blockFactory",
        "sharedCsvMapper",
        "options",
        "format",
        "extensions",
        "resolvedSchema",
        "schemaSampleSize",
        "effectivePolicy",
        "canonicalConfig",
        "readConfig",
        "declaredDateFormats",
        "declaredProvenanceBinding",
        "directBlockEnabled",
        "configWarnings"
    );

    /** Internally mutable fields written during reads. */
    private static final Set<String> SHARED_MUTABLE_FIELDS = Set.of("counters");

    private enum WitherLifecycle {
        /**
         * Mints a fresh counter struct: only {@link CsvFormatReader#withFreshCounters()} should declare this.
         * The operator factory calls it once per {@code get(DriverContext)} so each operator gets an isolated scope.
         */
        OPERATOR_MINT_FORKS,
        /**
         * Shares the parent's counter struct. ALL ordinary withers — per-query and per-file alike — must declare
         * this. Isolation is not at the wither seam; it is at {@link CsvFormatReader#withFreshCounters()}.
         */
        SHARES_COUNTERS,
        /** SPI default that returns {@code this}: no copy, so no counter decision needed. */
        IDENTITY_NO_COPY
    }

    private static final Map<String, WitherLifecycle> WITHER_LIFECYCLE = Map.ofEntries(
        Map.entry("withFreshCounters", WitherLifecycle.OPERATOR_MINT_FORKS),
        Map.entry("withConfig", WitherLifecycle.SHARES_COUNTERS),
        Map.entry("withConfigTrackingConsumedKeys", WitherLifecycle.SHARES_COUNTERS),
        Map.entry("withOptions", WitherLifecycle.SHARES_COUNTERS),
        Map.entry("withSchema", WitherLifecycle.SHARES_COUNTERS),
        Map.entry("withDeclaredDateFormats", WitherLifecycle.SHARES_COUNTERS),
        Map.entry("withDeclaredProvenanceBinding", WitherLifecycle.SHARES_COUNTERS),
        Map.entry("withDirectBlockEnabled", WitherLifecycle.SHARES_COUNTERS),
        Map.entry("withReadConfig", WitherLifecycle.SHARES_COUNTERS),
        Map.entry("withPushedFilter", WitherLifecycle.IDENTITY_NO_COPY),
        Map.entry("withDeclaredTypeColumns", WitherLifecycle.IDENTITY_NO_COPY)
    );

    public void testEveryInstanceFieldIsClassified() {
        Set<String> unclassified = new TreeSet<>();
        Set<String> stale = new TreeSet<>();
        Set<String> seen = new TreeSet<>();
        for (Field f : CsvFormatReader.class.getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers()) || f.isSynthetic()) {
                continue;
            }
            seen.add(f.getName());
            boolean immutable = IMMUTABLE_OR_CONFIG_FIELDS.contains(f.getName());
            boolean mutable = SHARED_MUTABLE_FIELDS.contains(f.getName());
            if (immutable == false && mutable == false) {
                unclassified.add(f.getName());
            }
            assertFalse("field [" + f.getName() + "] cannot be both immutable and shared-mutable", immutable && mutable);
        }
        assertTrue(
            "unclassified instance field(s) "
                + unclassified
                + ": decide the lifecycle — add to IMMUTABLE_OR_CONFIG_FIELDS if the field is never written after "
                + "construction, or to SHARED_MUTABLE_FIELDS (and declare every wither's behaviour for it) if a "
                + "wither's copy could share it across queries",
            unclassified.isEmpty()
        );
        for (String declared : IMMUTABLE_OR_CONFIG_FIELDS) {
            if (seen.contains(declared) == false) {
                stale.add(declared);
            }
        }
        for (String declared : SHARED_MUTABLE_FIELDS) {
            if (seen.contains(declared) == false) {
                stale.add(declared);
            }
        }
        assertTrue("stale classified field(s) " + stale + ": the reader no longer declares them", stale.isEmpty());
    }

    public void testEveryWitherDeclaresALifecycle() {
        Set<String> undeclared = new TreeSet<>();
        Set<String> found = new TreeSet<>();
        for (Method m : witherMethods()) {
            found.add(m.getName());
            if (WITHER_LIFECYCLE.containsKey(m.getName()) == false) {
                undeclared.add(m.getName());
            }
        }
        assertTrue(
            "wither(s) "
                + undeclared
                + " with no declared lifecycle: decide how it treats counters — OPERATOR_MINT_FORKS (only"
                + " withFreshCounters()), SHARES_COUNTERS (all ordinary withers), or IDENTITY_NO_COPY (returns"
                + " this) — add it to WITHER_LIFECYCLE and to sampleArgsFor(), and add a pin to the"
                + " status-snapshot suite",
            undeclared.isEmpty()
        );
        Set<String> stale = new TreeSet<>(WITHER_LIFECYCLE.keySet());
        stale.removeAll(found);
        assertTrue("stale WITHER_LIFECYCLE entr(ies) " + stale + ": no such wither on the reader any more", stale.isEmpty());
    }

    public void testWitherCopiesHonourTheDeclaredLifecycle() throws Exception {
        CsvFormatReader receiver = new CsvFormatReader(BLOCK_FACTORY);
        for (Method m : witherMethods()) {
            WitherLifecycle lifecycle = WITHER_LIFECYCLE.get(m.getName());
            assertNotNull("undeclared wither [" + m.getName() + "] — testEveryWitherDeclaresALifecycle reports these", lifecycle);
            Object product = invokeForcingACopy(m, receiver, lifecycle);
            switch (lifecycle) {
                case IDENTITY_NO_COPY -> assertSame(
                    "wither ["
                        + m.getName()
                        + "] is declared IDENTITY_NO_COPY but returned a copy: it now has state, so decide its"
                        + " lifecycle — reclassify it OPERATOR_MINT_FORKS or SHARES_COUNTERS",
                    receiver,
                    product
                );
                case OPERATOR_MINT_FORKS -> {
                    for (String field : SHARED_MUTABLE_FIELDS) {
                        assertNotSame(
                            "wither ["
                                + m.getName()
                                + "] is the operator mint so its copy must FORK ["
                                + field
                                + "]: sharing here gives the singleton the operator's counters",
                            fieldOf(receiver, field),
                            fieldOf(product, field)
                        );
                    }
                    Object sibling = invokeForcingACopy(m, receiver, lifecycle);
                    for (String field : SHARED_MUTABLE_FIELDS) {
                        assertNotSame(
                            "two mints from the same base share [" + field + "]: sibling operators would mix",
                            fieldOf(sibling, field),
                            fieldOf(product, field)
                        );
                    }
                }
                case SHARES_COUNTERS -> {
                    for (String field : SHARED_MUTABLE_FIELDS) {
                        assertSame(
                            "wither ["
                                + m.getName()
                                + "] must share ["
                                + field
                                + "] with its parent: forking here would leave the operator's reads unreported",
                            fieldOf(receiver, field),
                            fieldOf(product, field)
                        );
                    }
                }
            }
        }
    }

    /**
     * Pins the documented pass-through: an empty config returns the reader itself, so the "sibling configured
     * readers must not share" guarantee holds only for non-empty configs.
     */
    public void testEmptyConfigReturnsTheSameInstance() {
        CsvFormatReader receiver = new CsvFormatReader(BLOCK_FACTORY);
        assertSame(receiver, receiver.withConfigTrackingConsumedKeys(Map.of()).value());
    }

    /**
     * Invokes the wither with each candidate sample until one defeats the no-op short-circuits some withers have
     * (e.g. {@code withDirectBlockEnabled} returns {@code this} when the flag already matches). For copying
     * lifecycles a product identical to the receiver means every candidate hit a shortcut — fail loudly rather
     * than let the sharing assertion pass vacuously.
     */
    private static Object invokeForcingACopy(Method m, CsvFormatReader receiver, WitherLifecycle lifecycle) throws Exception {
        Object product = null;
        for (Object[] args : sampleArgsFor(m.getName())) {
            product = unwrap(m.invoke(receiver, args));
            if (product != receiver) {
                break;
            }
        }
        if (lifecycle != WitherLifecycle.IDENTITY_NO_COPY) {
            assertNotSame(
                "every sample-arg candidate for [" + m.getName() + "] hit a no-op shortcut; add one that forces a copy",
                receiver,
                product
            );
        }
        return product;
    }

    private static List<Method> witherMethods() {
        List<Method> methods = new ArrayList<>();
        for (Method m : CsvFormatReader.class.getMethods()) {
            if (m.isBridge() || m.isSynthetic()) {
                continue;
            }
            if (m.getName().startsWith("with") == false) {
                continue;
            }
            boolean returnsReader = FormatReader.class.isAssignableFrom(m.getReturnType());
            boolean returnsConfigured = m.getReturnType() == Configured.class;
            if (returnsReader || returnsConfigured) {
                methods.add(m);
            }
        }
        assertFalse("no withers found — the reflection filter is broken", methods.isEmpty());
        return methods;
    }

    private static Object unwrap(Object result) {
        return result instanceof Configured<?> configured ? configured.value() : result;
    }

    private static Object fieldOf(Object instance, String name) throws Exception {
        Field f = instance.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return f.get(instance);
    }

    private static List<Object[]> sampleArgsFor(String wither) {
        return switch (wither) {
            case "withFreshCounters" -> List.<Object[]>of(new Object[0]);
            case "withConfig", "withConfigTrackingConsumedKeys" -> List.<Object[]>of(new Object[] { Map.of("delimiter", "|") });
            case "withOptions" -> List.<Object[]>of(new Object[] { CsvFormatOptions.DEFAULT });
            case "withSchema" -> List.<Object[]>of(
                new Object[] { List.of(new ReferenceAttribute(Source.EMPTY, null, "a", DataType.LONG)) }
            );
            case "withDeclaredDateFormats" -> List.<Object[]>of(new Object[] { Map.of("b", "yyyy-MM-dd") });
            case "withDeclaredProvenanceBinding" -> List.<Object[]>of(new Object[] { true }, new Object[] { false });
            case "withDirectBlockEnabled" -> List.<Object[]>of(new Object[] { true }, new Object[] { false });
            case "withReadConfig" -> List.<Object[]>of(new Object[] { "0123456789abcdef0123456789abcdef" });
            case "withPushedFilter" -> List.<Object[]>of(new Object[] { new Object() });
            case "withDeclaredTypeColumns" -> List.<Object[]>of(new Object[] { Set.of("a") });
            default -> throw new AssertionError("update sampleArgsFor() for new wither: " + wither);
        };
    }
}
