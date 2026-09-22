/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
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
 * Lifecycle gate over {@link OrcFormatReader}'s mutable state. Counters are now passed via
 * {@code FormatReadContext} rather than carried as reader fields, so all ordinary withers are stateless copies.
 * <p>
 * The behavioural pins in {@link OrcReaderStatusTests} guard each known wither. This class guards the ENUMERATION:
 * a new wither or instance field added without deciding its lifecycle fails here rather than depending on someone
 * remembering to add a pin.
 */
@SuppressForbidden(reason = "reflection over declared fields and withers is the point: an undeclared one must not slip past the gate")
public class OrcFormatReaderStateLifecycleTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("noop"))
        .build();

    /**
     * Instance fields that carry no cross-instance mutable state: value-immutable, or never written after construction
     * and never written during a read. A new field must be added here or to {@link #SHARED_MUTABLE_FIELDS}.
     */
    private static final Set<String> IMMUTABLE_OR_CONFIG_FIELDS = Set.of(
        "parsedFooters",
        "footerBytes",
        "blockFactory",
        "pushedFilter",
        "pushedExpressions",
        "dynamicThreshold",
        "declaredDateFormats",
        "declaredTypeColumns"
    );

    /** Internally mutable fields written during reads. Counters are now passed via context, not stored as fields. */
    private static final Set<String> SHARED_MUTABLE_FIELDS = Set.of();

    private enum WitherLifecycle {
        /**
         * Copies the reader's configuration; all ordinary withers that produce a new instance declare this.
         */
        SHARES_COUNTERS,
        /** SPI default that returns {@code this}: no copy, so no state decision needed. */
        IDENTITY_NO_COPY
    }

    private static final Map<String, WitherLifecycle> WITHER_LIFECYCLE = Map.ofEntries(
        Map.entry("withPushedFilter", WitherLifecycle.SHARES_COUNTERS),
        Map.entry("withDynamicThreshold", WitherLifecycle.SHARES_COUNTERS),
        Map.entry("withDeclaredDateFormats", WitherLifecycle.SHARES_COUNTERS),
        Map.entry("withDeclaredTypeColumns", WitherLifecycle.SHARES_COUNTERS),
        Map.entry("withConfigTrackingConsumedKeys", WitherLifecycle.IDENTITY_NO_COPY),
        Map.entry("withConfig", WitherLifecycle.IDENTITY_NO_COPY),
        Map.entry("withSchema", WitherLifecycle.IDENTITY_NO_COPY),
        Map.entry("withDeclaredProvenanceBinding", WitherLifecycle.IDENTITY_NO_COPY),
        Map.entry("withReadConfig", WitherLifecycle.IDENTITY_NO_COPY)
    );

    public void testEveryInstanceFieldIsClassified() {
        Set<String> unclassified = new TreeSet<>();
        Set<String> stale = new TreeSet<>();
        Set<String> seen = new TreeSet<>();
        for (Field f : OrcFormatReader.class.getDeclaredFields()) {
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
                + " with no declared lifecycle: decide how it treats state — SHARES_COUNTERS (all ordinary withers)"
                + " or IDENTITY_NO_COPY (returns this) — add it to WITHER_LIFECYCLE and to sampleArgsFor(), and"
                + " add a pin to the status-snapshot suite",
            undeclared.isEmpty()
        );
        Set<String> stale = new TreeSet<>(WITHER_LIFECYCLE.keySet());
        stale.removeAll(found);
        assertTrue("stale WITHER_LIFECYCLE entr(ies) " + stale + ": no such wither on the reader any more", stale.isEmpty());
    }

    public void testWitherCopiesHonourTheDeclaredLifecycle() throws Exception {
        OrcFormatReader receiver = new OrcFormatReader(BLOCK_FACTORY);
        for (Method m : witherMethods()) {
            WitherLifecycle lifecycle = WITHER_LIFECYCLE.get(m.getName());
            assertNotNull("undeclared wither [" + m.getName() + "] — testEveryWitherDeclaresALifecycle reports these", lifecycle);
            Object product = unwrap(m.invoke(receiver, sampleArgsFor(m.getName())));
            switch (lifecycle) {
                case IDENTITY_NO_COPY -> assertSame(
                    "wither ["
                        + m.getName()
                        + "] is declared IDENTITY_NO_COPY but returned a copy: it now has state, so decide its"
                        + " lifecycle — reclassify it SHARES_COUNTERS",
                    receiver,
                    product
                );
                case SHARES_COUNTERS -> {
                    assertNotSame(
                        "sample args for [" + m.getName() + "] hit a no-op shortcut; use args that force a copy",
                        receiver,
                        product
                    );
                    for (String field : SHARED_MUTABLE_FIELDS) {
                        assertSame(
                            "wither [" + m.getName() + "] must share [" + field + "] with its parent",
                            fieldOf(receiver, field),
                            fieldOf(product, field)
                        );
                    }
                }
            }
        }
    }

    private static List<Method> witherMethods() {
        List<Method> methods = new ArrayList<>();
        for (Method m : OrcFormatReader.class.getMethods()) {
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

    private static Object[] sampleArgsFor(String wither) {
        return switch (wither) {
            // Non-null OrcPushedExpressions forces the copy branch (null with no existing filter returns this).
            case "withPushedFilter" -> new Object[] { new OrcPushedExpressions(List.of()) };
            // null is accepted by withDynamicThreshold and always produces a copy (no identity shortcut).
            case "withDynamicThreshold" -> new Object[] { null };
            case "withDeclaredDateFormats" -> new Object[] { Map.of("x", "yyyy-MM-dd") };
            case "withDeclaredTypeColumns" -> new Object[] { Set.of("x") };
            case "withConfigTrackingConsumedKeys", "withConfig" -> new Object[] { Map.of() };
            case "withSchema" -> new Object[] { List.of() };
            case "withDeclaredProvenanceBinding" -> new Object[] { false };
            case "withReadConfig" -> new Object[] { "" };
            default -> throw new AssertionError("update sampleArgsFor() for new wither: " + wither);
        };
    }
}
