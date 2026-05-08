/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.SuppressForbidden;
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
 *   <li>Each component's own {@code static final String CONFIG_*} string constants must match
 *       its {@code CONFIG_KEYS} set bidirectionally (reflection check below).
 *       Catches both 'added a new {@code CONFIG_X} constant but forgot to register' and
 *       'added an entry to {@code CONFIG_KEYS} that no constant declares (dead entry)'.
 * </ul>
 *
 * <p><b>Naming contract:</b> the bidirectional reflection check enumerates fields whose name starts
 * with {@code CONFIG_}. User-facing configuration keys MUST follow this prefix; otherwise the test
 * silently misses them (a {@code MY_OPTION_KEY}-style suffix would not be picked up). Internal
 * marker strings unrelated to user-facing config (split markers, file metadata keys, etc.) should
 * use a different naming pattern to keep the contract clear.
 *
 * <p>The generic validator contract lives in {@code ConfigKeyValidatorTests}.
 */
public class FileSourceFactoryValidationTests extends ESTestCase {

    public void testCoordinatorKeysIncludesFormatOverride() {
        assertTrue(FileSourceFactory.COORDINATOR_KEYS.contains(FileSourceFactory.CONFIG_FORMAT));
    }

    public void testCoordinatorKeysIncludesReaderOverride() {
        assertTrue(
            "FormatNameResolver.CONFIG_READER must be a coordinator key (the format-name resolver reads it)",
            FileSourceFactory.COORDINATOR_KEYS.contains(FormatNameResolver.CONFIG_READER)
        );
    }

    /**
     * Pins {@code StorageProviderRegistry.FRAMEWORK_KEYS ⊆ FileSourceFactory.COORDINATOR_KEYS}.
     * Both sets describe coordinator/framework-level config keys; if a future change adds a key
     * to FRAMEWORK_KEYS (for the storage-side strip step) but forgets the coordinator side, the
     * validator silently rejects every query that uses it. This unit-time check catches that
     * drift before it ships.
     */
    public void testFrameworkKeysAreSubsetOfCoordinatorKeys() {
        Set<String> missing = new TreeSet<>(StorageProviderRegistry.FRAMEWORK_KEYS);
        missing.removeAll(FileSourceFactory.COORDINATOR_KEYS);
        assertTrue("FRAMEWORK_KEYS not in COORDINATOR_KEYS: " + missing, missing.isEmpty());
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

    public void testErrorPolicyConfigKeysMatchConstants() {
        assertConfigKeysMatchConstants(ErrorPolicy.class, ErrorPolicy.CONFIG_KEYS);
    }

    public void testFileSplitProviderConfigKeysMatchConstants() {
        assertConfigKeysMatchConstants(FileSplitProvider.class, FileSplitProvider.CONFIG_KEYS);
    }

    /**
     * Asserts {@code declared} equals the set of values of every {@code static final String CONFIG_*}
     * constant declared on {@code clazz}. Reflection-based so new constants are picked up without
     * further test changes.
     */
    @SuppressForbidden(reason = "test-only reflection over CONFIG_* constants to pin set/constant symmetry")
    private static void assertConfigKeysMatchConstants(Class<?> clazz, Set<String> declared) {
        Set<String> fromConstants = new TreeSet<>();
        for (Field f : clazz.getDeclaredFields()) {
            int mods = f.getModifiers();
            if (Modifier.isStatic(mods) == false || Modifier.isFinal(mods) == false) {
                continue;
            }
            if (f.getType() != String.class) {
                continue;
            }
            if (f.getName().startsWith("CONFIG_") == false) {
                continue;
            }
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
        missingFromKeys.removeAll(declared);
        Set<String> extraInKeys = new TreeSet<>(declared);
        extraInKeys.removeAll(fromConstants);
        assertTrue(clazz.getSimpleName() + " CONFIG_* constants missing from CONFIG_KEYS: " + missingFromKeys, missingFromKeys.isEmpty());
        assertTrue(clazz.getSimpleName() + " CONFIG_KEYS entries with no backing CONFIG_* constant: " + extraInKeys, extraInKeys.isEmpty());
    }
}
