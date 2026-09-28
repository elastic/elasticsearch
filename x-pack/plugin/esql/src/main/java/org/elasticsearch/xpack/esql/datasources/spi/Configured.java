/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Carrier for "configure with this map" operations: a value, the keys it consumed, and the identity of
 * what it consumed.
 * <p>
 * {@code identity} is what a cache key uses in place of asking the configuration map itself. Only the
 * party being configured knows which of its settings change what it produces, so only that party can
 * say what identifies one configuration of it from another. A cache that decides this for everyone has
 * to keep a list of settings it does not own, and nothing checks that list against the readers and
 * providers it describes.
 * <p>
 * The value is opaque and node-stable: a consumer folds it into a key and never parses it. Empty means
 * nothing was consumed, which is the correct identity for a participant that takes no configuration.
 */
public record Configured<T>(T value, Set<String> consumedKeys, String identity) {

    public Configured {
        consumedKeys = Set.copyOf(Objects.requireNonNullElse(consumedKeys, Set.of()));
        identity = Objects.requireNonNullElse(identity, "");
    }

    public static <T> Configured<T> empty(T value) {
        return new Configured<>(value, Set.of(), "");
    }

    /**
     * Pairs {@code value} with the subset of {@code config}'s keys that match {@code recognized}, and
     * with the identity of those entries.
     */
    public static <T> Configured<T> fromKnownSubset(T value, Map<String, Object> config, Set<String> recognized) {
        return fromKnownSubset(value, config, recognized, Set.of());
    }

    /**
     * As {@link #fromKnownSubset(Object, Map, Set)}, with {@code identityInert} naming keys that are
     * consumed but do not change what this participant produces — a tuning hint, a buffer size. They
     * stay out of the identity so two configurations differing only in one of them share a cache entry.
     * <p>
     * Naming a key here is a claim that it cannot change a single row or value. Getting that wrong in
     * this direction lets two different reads share one record, so the default is to name nothing.
     */
    public static <T> Configured<T> fromKnownSubset(
        T value,
        Map<String, Object> config,
        Set<String> recognized,
        Set<String> identityInert
    ) {
        if (config == null || config.isEmpty()) {
            return Configured.empty(value);
        }
        // Stream straight into an unmodifiable set so the compact constructor's Set.copyOf is a no-op.
        Set<String> consumed = config.keySet().stream().filter(recognized::contains).collect(Collectors.toUnmodifiableSet());
        return new Configured<>(value, consumed, identityOf(config, consumed, identityInert));
    }

    /**
     * The canonical rendering every participant's identity uses: the named entries sorted by key, each
     * key and value length-prefixed.
     * <p>
     * Sorted because a map's iteration order is not part of a configuration. Length-prefixed because
     * without it a value containing the separator encodes identically to two entries, which is the
     * discipline {@code ReadConfigFingerprint} and {@code DefinitionVersion} already follow. Rendered
     * rather than hashed: the pre-image is bounded by a closed setting vocabulary, and a readable value
     * is worth more in a diagnostic than the bytes it saves.
     */
    public static String identityOf(Map<String, Object> config, Set<String> names, Set<String> excluded) {
        if (config == null || names == null || names.isEmpty()) {
            return "";
        }
        Map<String, String> sorted = new TreeMap<>();
        for (String name : names) {
            if (excluded != null && excluded.contains(name)) {
                continue;
            }
            Object value = config.get(name);
            sorted.put(name, value == null ? null : String.valueOf(value));
        }
        if (sorted.isEmpty()) {
            return "";
        }
        StringBuilder out = new StringBuilder();
        for (Map.Entry<String, String> entry : sorted.entrySet()) {
            appendLengthPrefixed(out, entry.getKey());
            appendLengthPrefixed(out, entry.getValue());
        }
        return out.toString();
    }

    /** A null value is distinguishable from an empty one, so an absent setting cannot imitate a blank. */
    private static void appendLengthPrefixed(StringBuilder out, String value) {
        if (value == null) {
            out.append("-1:");
        } else {
            out.append(value.length()).append(':').append(value);
        }
    }
}
