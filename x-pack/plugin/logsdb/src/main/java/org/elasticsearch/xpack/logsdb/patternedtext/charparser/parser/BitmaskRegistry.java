/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A registry that allocates and maps bits to {@link ParsingType} instances.
 * The order of registration is important because the earlier a type is registered, the lower its priority in the registry.
 * This corresponds to the bit index in the bitmask: as the bit index reflects the order of registration, it also reflects the priority of
 * the corresponding instance. The first registered instance will have the lowest bit index (0), and thus the lowest priority.
 * This means that when examining a bitmask with multiple bits set, the leftmost bit has the highest priority in that bitmask.
 * See {@link #getLeftmostBitIndex(int)} and {@link #getHighestPriorityType(int)} as the APIs to retrieve the highest priority type.
 * Therefore, register lower priority types (typically - more generic ones) first, and higher priority types later.
 *
 * @param <T> the type to register
 */
public final class BitmaskRegistry<T extends ParsingType> {
    private final Map<T, Integer> typeToBitmask;

    // array for quick access to instances by bit index
    private final T[] typesByBitIndex;

    // a two-dimensional array for quick access to higher level bitmasks by position for each type
    // the first dimension is the position, and the second dimension is the type bit index
    // for example, higherLevelBitmaskByPosition[2][3] will give the higher level bitmask for the type at bit index 3 at position 2
    private int[][] higherLevelBitmaskByPosition;

    private volatile int nextBitIndex;
    private volatile int accumulativeBitmask;
    private int combinedBitmask;

    private volatile boolean sealed = false;

    @SuppressWarnings("unchecked")
    public BitmaskRegistry() {
        this.typeToBitmask = new HashMap<>();
        // noinspection unchecked - we maintain type safety through APIs, the array is private an inaccessible otherwise
        this.typesByBitIndex = (T[]) new ParsingType[32]; // 32 bits for integer bitmask
        this.nextBitIndex = 0;
        this.accumulativeBitmask = 0;
        this.combinedBitmask = 0;
    }

    /**
     * Registers a {@link ParsingType} instance and allocates a bit for it.
     * Later registrations will have higher priority.
     * Types can only be registered before sealing the registry. See {@link #seal()} for more details.
     *
     * @param type the type to register
     * @return the bitmask for the registered type, where only one bit is set
     * @throws IllegalStateException if more than 32 types are registered
     * @throws IllegalArgumentException if the type is already registered
     */
    public synchronized int register(T type) {
        if (sealed) {
            throw new IllegalStateException("Cannot register new types after sealing the registry");
        }

        if (nextBitIndex >= 32) {
            throw new IllegalStateException("Cannot register more than 32 instances due to integer bit limit");
        }
        if (typeToBitmask.containsKey(type)) {
            throw new IllegalArgumentException("Type is already registered: " + type);
        }

        int bitIndex = nextBitIndex++;
        int bitmask = 1 << bitIndex;

        typeToBitmask.put(type, bitmask);
        typesByBitIndex[bitIndex] = type;
        accumulativeBitmask |= bitmask;

        // update higher level bitmask by position
        if (type.higherLevelBitmaskByPosition != null) {
            int numPositions = type.higherLevelBitmaskByPosition.length;
            ensureHigherLevelBitmaskByPositionCapacity(numPositions - 1);
            for (int position = 0; position < numPositions; position++) {
                higherLevelBitmaskByPosition[position][bitIndex] = type.higherLevelBitmaskByPosition[position];
            }
        }

        return bitmask;
    }

    private void ensureHigherLevelBitmaskByPositionCapacity(int position) {
        if (higherLevelBitmaskByPosition == null) {
            higherLevelBitmaskByPosition = new int[position + 1][32];
        } else if (higherLevelBitmaskByPosition.length <= position) {
            int[][] newArray = new int[position + 1][32];
            System.arraycopy(higherLevelBitmaskByPosition, 0, newArray, 0, higherLevelBitmaskByPosition.length);
            higherLevelBitmaskByPosition = newArray;
        }
    }

    /**
     * Seals the registry, preventing further registrations.
     * This is useful to ensure that the bitmask does not change after a certain point,
     * allowing for safe concurrent access.
     */
    public synchronized void seal() {
        if (sealed) {
            throw new IllegalStateException("Registry is already sealed");
        }
        combinedBitmask = accumulativeBitmask;
        sealed = true;
    }

    /**
     * Checks if the registry is sealed.
     *
     * @return true if the registry is sealed, false otherwise
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean isSealed() {
        return sealed;
    }

    /**
     * NOTE: not an optimized API
     * Gets the bit index associated with the given type.
     *
     * @param type the type to look up
     * @return the bit index for the given type
     * @throws IllegalArgumentException if the type is not registered
     */
    @NonOptimizedAPI
    public int getBitIndex(T type) {
        Integer bitmask = typeToBitmask.get(type);
        if (bitmask == null) {
            throw new IllegalArgumentException("Type is not registered: " + type);
        }
        return Integer.numberOfTrailingZeros(bitmask);
    }

    /**
     * Returns an unmodifiable collection with a view of all registered types.
     * @return an unmodifiable collection view of all registered types
     */
    @NonOptimizedAPI
    public Collection<T> getAllRegisteredTypes() {
        return Set.copyOf(typeToBitmask.keySet());
    }

    /**
     * NOTE: not an optimized API
     * Gets the bitmask associated with the given type.
     *
     * @param type the type to look up
     * @return the bitmask for the type
     * @throws IllegalArgumentException if the type is not registered
     */
    @NonOptimizedAPI
    public int getBitmask(T type) {
        Integer bitmask = typeToBitmask.get(type);
        if (bitmask == null) {
            throw new IllegalArgumentException("Type is not registered: " + type);
        }
        return bitmask;
    }

    /**
     * An optimized API to get the combined higher level bitmask that corresponds to the given bitmask and position.
     * The provided bitmask represents all valid types in this level, and the position indicates is used to look up which higher level
     * bits are valid for the given position through {@link ParsingType#getHigherLevelBitmaskByPosition(int)}.
     * For example, if this is a sub-token type registry, this method will return the combined bitmask of all token types that are valid
     * for all sub-tokens types represented by the given bitmask at the specified position.
     *
     * @param bitmask the bitmask representing the valid types in this level
     * @param position the position of this level instance within its higher-level entity
     * @return the combined higher level bitmask for the given bitmask and position
     */
    @OptimizedAPI
    public int getHigherLevelBitmaskByPositionOld(int bitmask, int position) {
        int resultBitmask = 0;
        int currentBitIndex = 0;
        while (bitmask != 0) {
            if ((bitmask & 1) != 0) {
                resultBitmask |= typesByBitIndex[currentBitIndex].getHigherLevelBitmaskByPosition(position);
            }
            bitmask >>>= 1;
            currentBitIndex++;
        }
        return resultBitmask;
    }

    @OptimizedAPI
    public int getHigherLevelBitmaskByPosition(int bitmask, int position) {
        int[] higherLevelBitmaskForPosition = higherLevelBitmaskByPosition[position];
        int resultBitmask = 0;
        int currentBitIndex = 0;
        while (bitmask != 0) {
            if ((bitmask & 1) != 0) {
                // if the rightmost bit is set - update the higher-level bitmask for the current bit index
                resultBitmask |= higherLevelBitmaskForPosition[currentBitIndex];
            }
            bitmask >>>= 1;
            currentBitIndex++;
        }
        return resultBitmask;
    }

    /**
     * NOTE: not an optimized API
     * Gets the bitmask associated with the given type name.
     *
     * @param name the name of the type to look up
     * @return the bitmask for the type with the given name
     * @throws IllegalArgumentException if no type is registered with the given name
     */
    @NonOptimizedAPI
    public int getBitmask(String name) {
        for (Map.Entry<T, Integer> entry : typeToBitmask.entrySet()) {
            if (entry.getKey().name().equals(name)) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException("No type is registered with name: " + name);
    }

    /**
     * Returns a bitmask with all bits corresponding to registered types turned on.
     *
     * @return the combined bitmask of all registered types
     */
    @OptimizedAPI
    public int getCombinedBitmask() {
        return combinedBitmask;
    }

    /**
     * NOTE: not an optimized API
     * Retrieves the type associated with the given name.
     *
     * @param name the name of the type to look up
     * @return the type associated with the given name
     * @throws IllegalArgumentException if no type is registered with the given name
     */
    @NonOptimizedAPI
    public T getTypeByName(final String name) {
        for (T type : typeToBitmask.keySet()) {
            if (type.name().equals(name)) {
                return type;
            }
        }
        throw new IllegalArgumentException("No type is registered with name: " + name);
    }

    /**
     * Optimized API for retrieving the type associated with the given bit index.
     *
     * @param bitIndex the bit index to look up
     * @return the type associated with the given bit index
     */
    @OptimizedAPI
    public T getTypeByBitIndex(final int bitIndex) {
        return typesByBitIndex[bitIndex];
    }

    /**
     * Optimized API to get the leftmost (the highest priority) bit's index from the given bitmask.
     * @param bitmask the bitmask to examine
     * @return the index of the leftmost bit
     */
    @OptimizedAPI
    public static int getLeftmostBitIndex(final int bitmask) {
        return 31 - Integer.numberOfLeadingZeros(bitmask);
    }

    /**
     * Optimized API to get the type associated with the leftmost (the highest priority) bit in the given bitmask.
     * This method expects a non-zero bitmask, otherwise it will throw an exception.
     *
     * @param bitmask the bitmask to examine
     * @return the type associated with the leftmost bit in the bitmask
     */
    @OptimizedAPI
    public T getHighestPriorityType(final int bitmask) {
        return typesByBitIndex[getLeftmostBitIndex(bitmask)];
    }

    /**
     * Optimized API to get the unique type associated with the given bitmask.
     * If the bitmask doesn't have exactly one bit set, it will return {@code null}.
     *
     * @param bitmask the bitmask to examine
     * @return the unique type associated with the given bitmask, or {@code null} if the bitmask doesn't have exactly one bit set
     */
    @OptimizedAPI
    public T getUniqueType(final int bitmask) {
        if (bitmask == 0) {
            return null;
        }
        if ((bitmask & (bitmask - 1)) != 0) {
            // more than one bit is set
            return null;
        }
        return getHighestPriorityType(bitmask);
    }

    /**
     * Returns the number of types registered in this registry.
     *
     * @return the number of registered types
     */
    public int size() {
        return typeToBitmask.size();
    }

    /**
     * An annotation to indicate optimized APIs.
     * This annotation is retained only at compile time.
     */
    @Retention(RetentionPolicy.CLASS)
    @Target(ElementType.METHOD)
    public @interface OptimizedAPI {
        // This annotation is used to mark methods that are optimized for performance.
        // Make sure to use only methods annotated with this during the parsing process.
    }

    /**
     * An annotation to indicate NON-optimized APIs.
     * This annotation is retained only at compile time.
     */
    @Retention(RetentionPolicy.CLASS)
    @Target(ElementType.METHOD)
    public @interface NonOptimizedAPI {
        // This annotation is used to mark methods that are not optimized for performance.
        // Refrain from using these methods during the parsing process.
    }
}
