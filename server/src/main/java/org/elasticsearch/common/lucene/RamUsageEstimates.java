/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.SuppressForbidden;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Assert-guarded helpers for {@link RamUsageEstimator#shallowSizeOf(Object)} at {@link Accountable#ramBytesUsed()} call sites where the
 * shallow instance size is a complete accounting of retained heap. Assertions fire only when assertions are enabled.
 */
public final class RamUsageEstimates {

    /** Shallow instance size of an empty {@link HashMap}. */
    public static final long HASH_MAP_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(HashMap.class);

    /** Shallow instance size of an empty {@link HashSet} (includes its backing {@link HashMap}). */
    public static final long HASH_SET_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(HashSet.class);

    /** Shallow instance size of an empty {@link LinkedHashMap}. */
    public static final long LINKED_HASH_MAP_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(LinkedHashMap.class);

    /** Shallow instance size of an empty {@link ArrayList}. */
    public static final long ARRAY_LIST_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ArrayList.class);

    /** Shallow instance size of an empty {@link TreeMap}. */
    public static final long TREE_MAP_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TreeMap.class);

    private RamUsageEstimates() {}

    /**
     * Returns {@link RamUsageEstimator#shallowSizeOf(Object)} after asserting that every non-static instance field on {@code o}'s class
     * hierarchy is primitive.
     */
    public static long shallowSizeOfPrimitiveOnly(Object o) {
        assert o != null;
        assert objectHasOnlyPrimitiveFields(o.getClass())
            : shallowCompleteAssertionMessage(o.getClass(), ReferenceFieldPolicy.PRIMITIVE_ONLY);
        return RamUsageEstimator.shallowSizeOf(o);
    }

    /**
     * Returns {@link RamUsageEstimator#shallowSizeOf(Object)} after asserting that every non-static instance field is either primitive or
     * an enum. Enum constants are shared singletons, so only the reference slot is retained per instance.
     */
    public static long shallowSizeOfShallowComplete(Object o) {
        assert o != null;
        assert objectHasOnlyShallowCompleteFields(o.getClass())
            : shallowCompleteAssertionMessage(o.getClass(), ReferenceFieldPolicy.SHALLOW_COMPLETE);
        return RamUsageEstimator.shallowSizeOf(o);
    }

    /**
     * Size of a loosely typed value field: boxed primitives, {@link String}, and {@link Accountable} values use dedicated estimators;
     * all other types must be shallow-complete (primitives and enums only).
     */
    public static long sizeOfShallowCompleteValue(Object value) {
        if (value == null) {
            return 0;
        }
        return switch (value) {
            case Long l -> RamUsageEstimator.sizeOf(l);
            case Integer i -> RamUsageEstimator.sizeOf(i);
            case String s -> RamUsageEstimator.sizeOf(s);
            case Accountable a -> a.ramBytesUsed();
            default -> shallowSizeOfShallowComplete(value);
        };
    }

    /** Returns true if every non-static instance field on {@code clazz}'s hierarchy is primitive. */
    public static boolean objectHasOnlyPrimitiveFields(Class<?> clazz) {
        return unaccountedReferenceFields(clazz, ReferenceFieldPolicy.PRIMITIVE_ONLY).isEmpty();
    }

    /** Returns true if every non-static instance field is primitive or enum. */
    public static boolean objectHasOnlyShallowCompleteFields(Class<?> clazz) {
        return unaccountedReferenceFields(clazz, ReferenceFieldPolicy.SHALLOW_COMPLETE).isEmpty();
    }

    /**
     * Names of non-static, non-primitive fields declared directly on {@code clazz}. Inherited fields are the declaring class's
     * responsibility. Used by accountable field tests to ensure every reference field is explicitly classified.
     */
    @SuppressForbidden(reason = "test-only field introspection for accountable field tripwires")
    public static Set<String> referenceFieldNamesDeclaredOn(Class<?> clazz) {
        return Arrays.stream(clazz.getDeclaredFields())
            .filter(f -> Modifier.isStatic(f.getModifiers()) == false)
            .filter(f -> f.getType().isPrimitive() == false)
            .map(Field::getName)
            .collect(Collectors.toUnmodifiableSet());
    }

    private enum ReferenceFieldPolicy {
        PRIMITIVE_ONLY,
        SHALLOW_COMPLETE
    }

    @SuppressForbidden(reason = "assert-only field introspection for ramBytesUsed() tripwires")
    private static Set<Field> unaccountedReferenceFields(Class<?> clazz, ReferenceFieldPolicy policy) {
        Set<Field> refs = new LinkedHashSet<>();
        for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field field : c.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                if (field.getType().isPrimitive()) {
                    continue;
                }
                if (policy == ReferenceFieldPolicy.SHALLOW_COMPLETE && field.getType().isEnum()) {
                    continue;
                }
                refs.add(field);
            }
        }
        return refs;
    }

    private static String shallowCompleteAssertionMessage(Class<?> clazz, ReferenceFieldPolicy policy) {
        String fields = unaccountedReferenceFields(clazz, policy).stream()
            .map(f -> f.getName() + ":" + f.getType().getSimpleName())
            .collect(Collectors.joining(", "));
        return switch (policy) {
            case PRIMITIVE_ONLY -> clazz.getName()
                + " has non-primitive fields ["
                + fields
                + "]; use shallowSizeOfShallowComplete or account explicitly";
            case SHALLOW_COMPLETE -> clazz.getName()
                + " has reference fields beyond enums ["
                + fields
                + "]; implement Accountable or account explicitly";
        };
    }
}
