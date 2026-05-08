/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Set;
import java.util.TreeSet;

/**
 * Pins {@link FileSourceFactory#COORDINATOR_KEYS} membership against the keys other code in this
 * factory actually consumes. A missing entry would cause a real configuration option (e.g.
 * {@code error_mode}, {@code target_split_size}) to be flagged as unknown for every user.
 *
 * <p>Two layers of safety:
 * <ul>
 *   <li>Per-component {@code CONFIG_KEYS} sets must each appear in the union (this class).
 *   <li>Each component's own {@code static final String CONFIG_*} string constants must each
 *       appear in the component's own {@code CONFIG_KEYS} set (the reflection check below).
 *       This catches the "added a new {@code config.get("X")} read but forgot to register" mistake.
 * </ul>
 *
 * <p>The generic validator contract lives in {@code ConfigKeyValidatorTests}.
 */
public class FileSourceFactoryValidationTests extends ESTestCase {

    public void testCoordinatorKeysIncludesFormatOverride() {
        assertTrue(FileSourceFactory.COORDINATOR_KEYS.contains(FileSourceFactory.CONFIG_FORMAT));
    }

    public void testCoordinatorKeysIncludesAllErrorPolicyKeys() {
        for (String key : ErrorPolicy.CONFIG_KEYS) {
            assertTrue("ErrorPolicy key " + key + " must be a coordinator key", FileSourceFactory.COORDINATOR_KEYS.contains(key));
        }
    }

    public void testCoordinatorKeysIncludesAllFileSplitProviderKeys() {
        for (String key : FileSplitProvider.CONFIG_KEYS) {
            assertTrue("FileSplitProvider key " + key + " must be a coordinator key", FileSourceFactory.COORDINATOR_KEYS.contains(key));
        }
    }

    /**
     * Walks {@link ErrorPolicy} for {@code static final String CONFIG_*} constants whose value is
     * a single literal key, and asserts every one is in {@link ErrorPolicy#CONFIG_KEYS}. Catches
     * the "added a new {@code CONFIG_X} constant + a {@code config.get(CONFIG_X)} read but forgot
     * to register the key" mistake.
     */
    public void testErrorPolicyConfigConstantsRegistered() {
        Set<String> missing = unregisteredConstants(ErrorPolicy.class, ErrorPolicy.CONFIG_KEYS);
        assertTrue("ErrorPolicy CONFIG_* constants missing from CONFIG_KEYS: " + missing, missing.isEmpty());
    }

    /** Same idea, for {@link FileSplitProvider}. */
    public void testFileSplitProviderConfigConstantsRegistered() {
        Set<String> missing = unregisteredConstants(FileSplitProvider.class, FileSplitProvider.CONFIG_KEYS);
        assertTrue("FileSplitProvider CONFIG_* constants missing from CONFIG_KEYS: " + missing, missing.isEmpty());
    }

    /**
     * Returns the set of {@code static final String CONFIG_*} constants on {@code clazz} that are
     * NOT in {@code declared}. Skips {@code CONFIG_KEYS} itself (it's a Set, not a String). Reads
     * via reflection so future-added constants are picked up without further test changes.
     */
    private static Set<String> unregisteredConstants(Class<?> clazz, Set<String> declared) {
        Set<String> missing = new TreeSet<>();
        for (Field f : clazz.getDeclaredFields()) {
            int mods = f.getModifiers();
            if (Modifier.isStatic(mods) == false || Modifier.isFinal(mods) == false) {
                continue;
            }
            if (f.getType() != String.class) {
                continue;
            }
            String fname = f.getName();
            if (fname.startsWith("CONFIG_") == false) {
                continue;
            }
            f.setAccessible(true);
            try {
                String value = (String) f.get(null);
                if (value != null && declared.contains(value) == false) {
                    missing.add(fname + "=\"" + value + "\"");
                }
            } catch (IllegalAccessException e) {
                throw new AssertionError("cannot read constant " + fname, e);
            }
        }
        return missing;
    }
}
