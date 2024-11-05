/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.elasticsearch.Build;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomValueOtherThan;

/**
 * Utilities for copying and mutating Build instances in tests.
 */
public class BuildUtils {

    private static final MethodHandle buildCtor;
    static {
        try {
            buildCtor = MethodHandles.publicLookup().unreflectConstructor(Build.class.getConstructors()[0]);
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        }
    }

    // creates a new Build instance with the same members, except those in extraArgs are substituted

    /**
     * Creates a new Build instance, using and existing build instance as a template.
     *
     * @param existing A Build instance which will be copied
     * @param extraArgs A map of arguments to the Build constructor which will override values in the {@code existing} instance
     * @return A new Build instance
     */
    public static Build newBuild(Build existing, Map<String, Object> extraArgs) {
        var argsOverrides = new HashMap<>(extraArgs);
        RecordComponent[] argsComponents = Build.class.getRecordComponents();
        try {
            Object[] args = new Object[argsComponents.length];
            int i = 0;
            for (var argComponent : argsComponents) {
                Object value = argsOverrides.remove(argComponent.getName());
                if (value == null) {
                    value = argComponent.getAccessor().invoke(existing);
                }
                args[i++] = value;
            }

            if (argsOverrides.isEmpty() == false) {
                throw new AssertionError("Unknown Build arguments: " + argsOverrides.keySet());
            }

            return (Build) buildCtor.invokeWithArguments(args);
        } catch (Throwable t) {
            throw new AssertionError("Failed to create test Build instance", t);
        }
    }

    /**
     * Creates a random mutation of the given Build instance.
     *
     * The existing build instance is copied with a single field randomly changed. The value for the field is randomized.
     * @param existing The existing instance to mutate
     * @return A new Build instance
     */
    public static Build mutateBuild(Build existing) {
        RecordComponent[] argsComponents = Build.class.getRecordComponents();
        var argToChange = argsComponents[randomInt(argsComponents.length - 1)];
        Class<?> argType = argToChange.getType();
        final Object currentValue;
        try {
            currentValue = argToChange.getAccessor().invoke(existing);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new AssertionError(e);
        }
        final Object newValue;
        if (argType.isEnum()) {
            newValue = randomValueOtherThan(currentValue, () -> randomFrom(argType.getEnumConstants()));
        } else if (argType.equals(String.class)) {
            newValue = randomStringExcept((String) currentValue);
        } else if (argType.equals(boolean.class)) {
            newValue = ((Boolean) currentValue) == false;
        } else {
            throw new AssertionError("Build has unknown arg type: " + argType.getName());
        }
        return newBuild(existing, Map.of(argToChange.getName(), newValue));
    }

    private static String randomStringExcept(final String s) {
        int len = s == null ? 0 : s.length();
        return randomAlphaOfLength(13 - len);
    }
}
