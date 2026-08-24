/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.cache.SchemaCacheKey;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatSpec;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

/** Pins {@link ParquetFormatReader#RECOGNIZED_KEYS} against the parser's actual reads. */
public class ParquetFormatReaderRecognizedKeysTests extends ESTestCase {

    private static final BlockFactory NOOP_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("noop"))
        .build();

    public void testRecognizedKeysSetIsExpected() {
        Set<String> expected = new TreeSet<>();
        expected.add("optimized_reader");
        expected.add("late_materialization");
        assertEquals(expected, new TreeSet<>(ParquetFormatReader.RECOGNIZED_KEYS));
    }

    public void testEveryRecognizedKeyRoundTripsThroughWithConfig() {
        for (String key : ParquetFormatReader.RECOGNIZED_KEYS) {
            Map<String, Object> config = new HashMap<>();
            config.put(key, true);
            Configured<FormatReader> result = new ParquetFormatReader(NOOP_BLOCK_FACTORY).withConfigTrackingConsumedKeys(config);
            assertTrue("key [" + key + "] must be consumed when present", result.consumedKeys().contains(key));
        }
    }

    public void testUnknownKeysAreNotClaimed() {
        Map<String, Object> config = new HashMap<>();
        config.put("optimized_reader", true);
        config.put("not_a_parquet_key", true);
        Configured<FormatReader> result = new ParquetFormatReader(NOOP_BLOCK_FACTORY).withConfigTrackingConsumedKeys(config);
        assertThat(result.consumedKeys(), containsInAnyOrder("optimized_reader"));
    }

    public void testEmptyConfigConsumesNothing() {
        assertThat(new ParquetFormatReader(NOOP_BLOCK_FACTORY).withConfigTrackingConsumedKeys(Map.of()).consumedKeys(), empty());
    }

    public void testNullConfigConsumesNothing() {
        assertThat(new ParquetFormatReader(NOOP_BLOCK_FACTORY).withConfigTrackingConsumedKeys(null).consumedKeys(), empty());
    }

    /**
     * Bidirectional symmetry: every {@code static final String CONFIG_*} constant on
     * {@link ParquetFormatReader} appears in {@link ParquetFormatReader#RECOGNIZED_KEYS}, and
     * every entry in {@code RECOGNIZED_KEYS} is backed by a matching constant. Catches both
     * 'added a new {@code CONFIG_X} constant + a {@code config.get(CONFIG_X)} read but forgot
     * to register' and 'added a string to {@code RECOGNIZED_KEYS} with no backing constant
     * (likely dead)'.
     */
    @SuppressForbidden(reason = "test-only reflection over CONFIG_* constants to pin set/constant symmetry")
    public void testRecognizedKeysMatchConfigConstants() {
        Set<String> fromConstants = new TreeSet<>();
        for (Field f : ParquetFormatReader.class.getDeclaredFields()) {
            int mods = f.getModifiers();
            if (Modifier.isStatic(mods) == false || Modifier.isFinal(mods) == false) continue;
            if (f.getType() != String.class) continue;
            if (f.getName().startsWith("CONFIG_") == false) continue;
            f.setAccessible(true);
            try {
                String value = (String) f.get(null);
                if (value != null) {
                    fromConstants.add(value);
                }
            } catch (IllegalAccessException e) {
                throw new AssertionError("cannot read constant " + f.getName(), e);
            }
        }
        Set<String> missingFromKeys = new TreeSet<>(fromConstants);
        missingFromKeys.removeAll(ParquetFormatReader.RECOGNIZED_KEYS);
        Set<String> extraInKeys = new TreeSet<>(ParquetFormatReader.RECOGNIZED_KEYS);
        extraInKeys.removeAll(fromConstants);
        assertTrue("ParquetFormatReader CONFIG_* constants missing from RECOGNIZED_KEYS: " + missingFromKeys, missingFromKeys.isEmpty());
        assertTrue("ParquetFormatReader RECOGNIZED_KEYS entries with no backing CONFIG_* constant: " + extraInKeys, extraInKeys.isEmpty());
    }

    /**
     * Verifies that the config keys declared in {@link ParquetDataSourcePlugin#FORMAT_CONFIG_KEYS}
     * (used for CRUD-time validation via {@link FormatSpec#configKeys()}) match the reader's
     * runtime {@link ParquetFormatReader#RECOGNIZED_KEYS} exactly.
     */
    public void testFormatSpecConfigKeysMatchRecognizedKeys() {
        Set<String> specKeys = new TreeSet<>(ParquetDataSourcePlugin.FORMAT_CONFIG_KEYS);
        Set<String> readerKeys = new TreeSet<>(ParquetFormatReader.RECOGNIZED_KEYS);
        Set<String> missingFromSpec = new TreeSet<>(readerKeys);
        missingFromSpec.removeAll(specKeys);
        Set<String> extraInSpec = new TreeSet<>(specKeys);
        extraInSpec.removeAll(readerKeys);
        assertTrue(
            "ParquetFormatReader.RECOGNIZED_KEYS has keys missing from FormatSpec.configKeys: " + missingFromSpec,
            missingFromSpec.isEmpty()
        );
        assertTrue("FormatSpec.configKeys has keys not in ParquetFormatReader.RECOGNIZED_KEYS: " + extraInSpec, extraInSpec.isEmpty());
    }

    /**
     * Verifies that every FormatSpec declared by the plugin carries the config keys.
     */
    public void testAllFormatSpecsDeclareConfigKeys() {
        ParquetDataSourcePlugin plugin = new ParquetDataSourcePlugin();
        for (FormatSpec spec : plugin.formatSpecs()) {
            assertEquals(
                "FormatSpec for [" + spec.format() + "] must declare FORMAT_CONFIG_KEYS",
                ParquetDataSourcePlugin.FORMAT_CONFIG_KEYS,
                spec.configKeys()
            );
        }
    }

    /**
     * Every key the reader consumes must either participate in the cache identity
     * ({@link SchemaCacheKey#affectsIdentity}) or be declared inert here with a justification.
     * <ul>
     *   <li>{@code optimized_reader} — selects the reader implementation; both decode the same values from the
     *       same footer-described pages.</li>
     *   <li>{@code late_materialization} — selects when projected columns are materialised relative to filtering;
     *       the rows and values produced are identical either way.</li>
     * </ul>
     */
    private static final Set<String> IDENTITY_INERT_KEYS = Set.of(
        ParquetFormatReader.CONFIG_OPTIMIZED_READER,
        ParquetFormatReader.CONFIG_LATE_MATERIALIZATION
    );

    public void testEveryRecognizedKeyIsIdentityAffectingOrDeclaredInert() {
        for (String key : ParquetFormatReader.RECOGNIZED_KEYS) {
            boolean affects = SchemaCacheKey.affectsIdentity(key);
            boolean inert = IDENTITY_INERT_KEYS.contains(key);
            assertTrue(
                "key ["
                    + key
                    + "] is consumed by the reader but neither participates in the cache identity nor is declared "
                    + "inert: add it to SchemaCacheKey's identity params, or declare it in IDENTITY_INERT_KEYS with "
                    + "a justification that it cannot change which rows survive or what values they hold",
                affects || inert
            );
            assertFalse("key [" + key + "] cannot be both identity-affecting and declared inert", affects && inert);
        }
    }

    public void testDeclaredInertKeysAreStillRecognized() {
        for (String key : IDENTITY_INERT_KEYS) {
            assertTrue(
                "stale IDENTITY_INERT_KEYS entry [" + key + "]: the reader no longer consumes it",
                ParquetFormatReader.RECOGNIZED_KEYS.contains(key)
            );
        }
    }
}
