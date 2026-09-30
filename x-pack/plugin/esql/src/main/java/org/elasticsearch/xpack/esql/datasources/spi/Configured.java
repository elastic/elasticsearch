/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.encryption.spi.EncryptedData;

import java.util.Base64;
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
public record Configured<T>(T value, Set<String> consumedKeys, String identity, String secretIdentity) {

    public Configured {
        consumedKeys = Set.copyOf(Objects.requireNonNullElse(consumedKeys, Set.of()));
        identity = Objects.requireNonNullElse(identity, "");
        secretIdentity = Objects.requireNonNullElse(secretIdentity, "");
    }

    // There is deliberately no three-argument convenience constructor. One existed for a few minutes and
    // StorageProviderFactory silently dropped the secret identity through it, which is the same failure this class
    // exists to remove: a participant that reports what it consumed and silently reports no identity. A participant
    // with no secrets passes "" and says so.

    public static <T> Configured<T> empty(T value) {
        return new Configured<>(value, Set.of(), "", "");
    }

    /**
     * Pairs {@code value} with the subset of {@code config}'s keys that match {@code recognized}, and
     * with the identity of those entries.
     */
    public static <T> Configured<T> fromKnownSubset(T value, Map<String, Object> config, Set<String> recognized) {
        return fromKnownSubset(value, config, recognized, Set.of());
    }

    /**
     * As {@link #fromKnownSubset(Object, Map, Set, Set)}, but with the identity supplied rather than derived — for a
     * participant whose identity carries something beyond the named keys.
     * <p>
     * A text reader's identity folds in its resolved error policy, because the policy decides which rows survive and
     * so it identifies the read. That value is also the fingerprint the reader stamps on a harvest, and the two must
     * be the same string: the coordinator seeds a cache entry with the identity the reader vends, and the data node
     * stamps the harvest, and {@code ExternalSourceCacheService.matchesContribution} enriches the entry only when
     * they compare equal. Deriving them separately is how a strict dataset stopped warming while every assertion
     * about correctness stayed green.
     */
    public static <T> Configured<T> fromKnownSubsetWithIdentity(
        T value,
        Map<String, Object> config,
        Set<String> recognized,
        String identity
    ) {
        if (config == null || config.isEmpty()) {
            return Configured.empty(value);
        }
        Set<String> consumed = config.keySet().stream().filter(recognized::contains).collect(Collectors.toUnmodifiableSet());
        return new Configured<>(value, consumed, identity, "");
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
        return new Configured<>(value, consumed, identityOf(config, consumed, identityInert), "");
    }

    /**
     * The identity of whichever of {@code recognized} this config actually carries. The entry point for a
     * participant that has its recognised set in hand but is not going through {@link #fromKnownSubset} —
     * a reader deriving the value it stamps on a harvest, or a coordinator identifying its own keys.
     */
    public static String identityOf(Map<String, Object> config, Set<String> recognized) {
        if (config == null || config.isEmpty() || recognized == null || recognized.isEmpty()) {
            return "";
        }
        return identityOf(config, recognized, Set.of());
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
            // Absent rather than null: a setting the config does not carry contributes nothing, so two configs
            // differing only in a setting neither sets have one identity.
            if (config.containsKey(name) == false || (excluded != null && excluded.contains(name))) {
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

    /**
     * The identity of the declared-secret settings this config carries, as a digest.
     * <p>
     * Separate from {@link #identity} because the two are consumed by different keys for opposite reasons. A
     * schema or file-metadata entry describes what a file *contains*, which does not depend on who read it, so a
     * credential must not fragment those addresses. A listing describes what a principal can *see*, which does,
     * so the listing key carries this.
     * <p>
     * Digested rather than rendered, which is the one place this class departs from {@link #identityOf}: a cache
     * key outlives the data source and is printed by {@code toString}, so the value that distinguishes two
     * credentials travels as {@link StorageIdentity#digestSecret} of the same length-prefixed pre-image and never
     * as the credential.
     * <p>
     * The set comes from the provider's own field definitions, never from a list written beside the cache. A
     * hand-written list of seven credential names carried {@code access_key} and {@code secret_key} and not
     * {@code session_token}, {@code role_arn} or {@code auth}, so two roles over one bucket addressed one listing.
     */
    public static String secretIdentityOf(Map<String, Object> config, Set<String> secretNames) {
        if (config == null || config.isEmpty() || secretNames == null || secretNames.isEmpty()) {
            return "";
        }
        Map<String, String> sorted = new TreeMap<>();
        for (String name : secretNames) {
            if (config.containsKey(name) == false) {
                continue;
            }
            sorted.put(name, renderSecret(config.get(name)));
        }
        if (sorted.isEmpty()) {
            return "";
        }
        StringBuilder preImage = new StringBuilder();
        for (Map.Entry<String, String> entry : sorted.entrySet()) {
            appendLengthPrefixed(preImage, entry.getKey());
            appendLengthPrefixed(preImage, entry.getValue());
        }
        return StorageIdentity.digestSecret(preImage.toString());
    }

    /**
     * A secret as something whose text changes whenever the value does. An {@link EncryptedData} carrier redacts
     * its ciphertext in {@code toString}, so letting it render itself would fold in only the field name and the
     * project key id and every rotation would produce one digest. A {@code byte[]} would render an identity hash,
     * so every deserialization would mint a new one.
     */
    private static String renderSecret(Object rawValue) {
        if (rawValue == null) {
            return null;
        }
        if (rawValue instanceof EncryptedData encrypted) {
            return encrypted.keyId() + ':' + Base64.getEncoder().encodeToString(encrypted.payload());
        }
        if (rawValue instanceof byte[] bytes) {
            return Base64.getEncoder().encodeToString(bytes);
        }
        return rawValue.toString();
    }

    /**
     * The identities of several participants as one value. Each is length-prefixed, so no pair of triples folds to
     * one string however the parts are spelled — a participant's value is user-influenced, and two keys colliding
     * here is a wrong answer rather than a slow query.
     */
    public static String fold(String... identities) {
        StringBuilder out = new StringBuilder();
        for (String identity : identities) {
            appendLengthPrefixed(out, identity);
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
