/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.cache.SchemaCacheKey;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.FormatSpec;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

/** Pins {@link NdJsonFormatReaderFactory#RECOGNIZED_KEYS} against the parser's actual reads. */
public class NdJsonFormatReaderRecognizedKeysTests extends ESTestCase {

    public void testRecognizedKeysSetIsExpected() {
        Set<String> expected = new TreeSet<>();
        expected.add("schema_sample_size");
        expected.add("segment_size");
        expected.add("datetime_format");
        assertEquals(expected, new TreeSet<>(NdJsonFormatReaderFactory.RECOGNIZED_KEYS));
    }

    public void testEveryRecognizedKeyRoundTripsThroughInspect() {
        NdJsonFormatReaderFactory factory = newFactory();
        for (String key : NdJsonFormatReaderFactory.RECOGNIZED_KEYS) {
            Map<String, Object> config = new HashMap<>();
            config.put(key, sampleValueFor(key));
            try {
                Configured<Void> result = factory.inspect(config);
                assertTrue("key [" + key + "] must be consumed when present", result.consumedKeys().contains(key));
            } catch (RuntimeException e) {
                // A throw still proves the key was read: the factory looked at it before rejecting.
            }
        }
    }

    public void testUnknownKeysAreNotClaimed() {
        Map<String, Object> config = new HashMap<>();
        config.put("schema_sample_size", "20");
        config.put("not_an_ndjson_key", true);
        Configured<Void> result = newFactory().inspect(config);
        assertThat(result.consumedKeys(), containsInAnyOrder("schema_sample_size"));
    }

    public void testEmptyConfigConsumesNothing() {
        assertThat(newFactory().inspect(Map.of()).consumedKeys(), empty());
    }

    public void testNullConfigConsumesNothing() {
        assertThat(newFactory().inspect(null).consumedKeys(), empty());
    }

    /**
     * Bidirectional symmetry: every {@code static final String CONFIG_*} constant on
     * {@link NdJsonFormatReaderFactory} appears in {@link NdJsonFormatReaderFactory#RECOGNIZED_KEYS}, and every
     * entry in {@code RECOGNIZED_KEYS} is backed by a matching constant.
     */
    @SuppressForbidden(reason = "test-only reflection over CONFIG_* constants to pin set/constant symmetry")
    public void testRecognizedKeysMatchConfigConstants() {
        Set<String> fromConstants = new TreeSet<>();
        for (Field f : NdJsonFormatReaderFactory.class.getDeclaredFields()) {
            int mods = f.getModifiers();
            if (Modifier.isStatic(mods) == false || Modifier.isFinal(mods) == false) continue;
            if (f.getType() != String.class) continue;
            if (f.getName().startsWith("CONFIG_") == false) continue;
            f.setAccessible(true);
            try {
                String value = (String) f.get(null);
                if (value != null) fromConstants.add(value);
            } catch (IllegalAccessException e) {
                throw new AssertionError("cannot read constant " + f.getName(), e);
            }
        }
        Set<String> missingFromKeys = new TreeSet<>(fromConstants);
        missingFromKeys.removeAll(NdJsonFormatReaderFactory.RECOGNIZED_KEYS);
        Set<String> extraInKeys = new TreeSet<>(NdJsonFormatReaderFactory.RECOGNIZED_KEYS);
        extraInKeys.removeAll(fromConstants);
        assertTrue(
            "NdJsonFormatReaderFactory CONFIG_* constants missing from RECOGNIZED_KEYS: " + missingFromKeys,
            missingFromKeys.isEmpty()
        );
        assertTrue(
            "NdJsonFormatReaderFactory RECOGNIZED_KEYS entries with no backing CONFIG_* constant: " + extraInKeys,
            extraInKeys.isEmpty()
        );
    }

    /**
     * Verifies that the config keys declared in {@link NdJsonDataSourcePlugin#FORMAT_CONFIG_KEYS}
     * (used for CRUD-time validation via {@link FormatSpec#configKeys()}) match the factory's
     * runtime {@link NdJsonFormatReaderFactory#RECOGNIZED_KEYS} exactly.
     */
    public void testFormatSpecConfigKeysMatchRecognizedKeys() {
        Set<String> specKeys = new TreeSet<>(NdJsonDataSourcePlugin.FORMAT_CONFIG_KEYS);
        Set<String> readerKeys = new TreeSet<>(NdJsonFormatReaderFactory.RECOGNIZED_KEYS);
        Set<String> missingFromSpec = new TreeSet<>(readerKeys);
        missingFromSpec.removeAll(specKeys);
        Set<String> extraInSpec = new TreeSet<>(specKeys);
        extraInSpec.removeAll(readerKeys);
        assertTrue(
            "NdJsonFormatReaderFactory.RECOGNIZED_KEYS has keys missing from FormatSpec.configKeys: " + missingFromSpec,
            missingFromSpec.isEmpty()
        );
        assertTrue(
            "FormatSpec.configKeys has keys not in NdJsonFormatReaderFactory.RECOGNIZED_KEYS: " + extraInSpec,
            extraInSpec.isEmpty()
        );
    }

    /**
     * Verifies that every FormatSpec declared by the plugin carries the config keys.
     */
    public void testAllFormatSpecsDeclareConfigKeys() {
        NdJsonDataSourcePlugin plugin = new NdJsonDataSourcePlugin();
        for (FormatSpec spec : plugin.formatSpecs()) {
            assertEquals(
                "FormatSpec for [" + spec.format() + "] must declare FORMAT_CONFIG_KEYS",
                NdJsonDataSourcePlugin.FORMAT_CONFIG_KEYS,
                spec.configKeys()
            );
        }
    }

    private static NdJsonFormatReaderFactory newFactory() {
        return new NdJsonFormatReaderFactory(Settings.EMPTY);
    }

    private static Object sampleValueFor(String key) {
        return switch (key) {
            case "schema_sample_size" -> 10;
            case "segment_size" -> "2mb";
            case "datetime_format" -> "dd/MM/yyyy HH:mm:ss";
            default -> throw new AssertionError("update sampleValueFor() for new recognised key: " + key);
        };
    }

    /**
     * Every key the factory consumes must either participate in the cache identity
     * ({@link SchemaCacheKey#affectsIdentity}) or be declared inert here with a justification.
     * <ul>
     *   <li>{@code segment_size}: read segmentation only. The split-alignment protocol (leading partial record
     *       dropped, trailing partial record finished) makes the surviving record set, and therefore every
     *       statistic over it, independent of where segments fall.</li>
     * </ul>
     */
    private static final Set<String> IDENTITY_INERT_KEYS = Set.of(NdJsonFormatReaderFactory.CONFIG_SEGMENT_SIZE);

    public void testEveryRecognizedKeyIsIdentityAffectingOrDeclaredInert() {
        for (String key : NdJsonFormatReaderFactory.RECOGNIZED_KEYS) {
            boolean affects = SchemaCacheKey.affectsIdentity(key);
            boolean inert = IDENTITY_INERT_KEYS.contains(key);
            assertTrue(
                "key ["
                    + key
                    + "] is consumed by the factory but neither participates in the cache identity nor is declared "
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
                "stale IDENTITY_INERT_KEYS entry [" + key + "]: the factory no longer consumes it",
                NdJsonFormatReaderFactory.RECOGNIZED_KEYS.contains(key)
            );
        }
    }
}
