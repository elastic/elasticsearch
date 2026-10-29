/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

/**
 * An immutable mapping from SubstringView to integer bitmask values.
 * todo: consider optimizations, mostly regarding equality checks of SubstringView keys - either through the equals implementation (e.g.
 *   failing fast on hashcode mismatch) and/or through making the map sparse (e.g. using a trie or a perfect hash function if the set of
 *   keys is known to be static and unchanging).
 */
public final class SubstringToIntegerMap implements ToIntFunction<SubstringView> {

    private final Map<SubstringView, Integer> map;

    private SubstringToIntegerMap(Map<SubstringView, Integer> map) {
        this.map = map;
    }

    @Override
    public int applyAsInt(final SubstringView input) {
        Integer value = map.get(input);
        // unboxing - no allocation (as opposed to boxing) and most likely ~0 overhead for primitive value retrieval
        return value != null ? value : 0;
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * Builder for creating SubstringToIntMap instances.
     */
    public static class Builder {
        private final Map<String, Integer> map;

        private Builder() {
            this.map = new HashMap<>();
        }

        /**
         * Get the current value mapped to the given key.
         * @param key the string key
         * @return the mapped integer value, or null if the key does not exist
         */
        public Integer get(String key) {
            return map.get(key);
        }

        /**
         * Check if the map being built is empty.
         * @return true if the map is empty, false otherwise
         */
        public boolean isEmpty() {
            return map.isEmpty();
        }

        /**
         * Add an entry to the map being built if such does not already exist. If the key already exists, the mapped bitmask is updated
         * by ORing the existing value with the new value.
         * @param key the string key
         * @param value the integer value
         * @return this builder for method chaining
         */
        public Builder add(String key, int value) {
            map.merge(key, value, (oldVal, newVal) -> oldVal | newVal);
            return this;
        }

        public Builder addAll(SubstringToIntegerMap other) {
            other.map.forEach((key, value) -> this.add(key.toString(), value));
            return this;
        }

        /**
         * Add all entries from another map to the map being built. See {@link #add(String, int)} for handling of key collisions.
         * @param otherMap the map whose entries are to be merged into this builder's map
         * @return this builder for method chaining
         */
        public Builder addAll(Map<String, Integer> otherMap) {
            otherMap.forEach(this::add);
            return this;
        }

        /**
         * Add all entries of the given set to the map being built, mapping each string to the given bitmask value.
         * If a key already exists, the mapped bitmask is updated by ORing the existing with the new one.
         * @param otherKeys the set of strings to be added
         * @param bitmask the bitmask value to map each string to
         * @return this builder for method chaining
         */
        public Builder addAll(Set<String> otherKeys, int bitmask) {
            otherKeys.forEach(key -> this.add(key, bitmask));
            return this;
        }

        /**
         * Build an immutable SubstringToIntMap from the accumulated entries.
         * @return a new SubstringToIntMap instance
         * @throws IllegalArgumentException if the map is empty
         */
        public SubstringToIntegerMap build() {
            if (map.isEmpty()) {
                throw new IllegalArgumentException("Map cannot be null or empty");
            }

            Map<SubstringView, Integer> substringViewMap = map.entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> new SubstringView(entry.getKey()), Map.Entry::getValue));

            return new SubstringToIntegerMap(Map.copyOf(substringViewMap));
        }
    }

    /**
     * Creates a new Builder instance.
     * @return a new Builder
     */
    public static Builder builder() {
        return new Builder();
    }
}
