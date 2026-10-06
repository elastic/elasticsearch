/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.DefinitionVersion;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * What every cached record about one dataset has in common: which reader produces it, and who the
 * participants are that decide what it holds.
 * <p>
 * It exists because those facts used to be repeated in every key of a dataset. A glob over ten thousand
 * files minted ten thousand keys each carrying its own copy of the format name, the folded participant
 * identity and the definition version, where one shared instance says the same thing: the keys of one
 * dataset hold the same {@code SourceScope} by reference, so comparing two of them is a reference check
 * before any field is looked at, and the hash is computed once for the dataset rather than once per key.
 * <p>
 * {@code producer} stays textual because it is read for its value - it is the {@code sourceType} the
 * resolved metadata reports. The participants do not: the folded identity and the definition version are
 * compared and never inspected, the one exception being a distinctness count in the reconcile's refusal
 * to enrich entries derived under more than one identity, which a fold answers exactly as the text did.
 * So they are carried as a 128-bit fold rather than as the strings they were folded from.
 * <p>
 * 128 bits, for the reason {@link org.elasticsearch.xpack.esql.datasources.FileSetFingerprint} gives: a
 * collision serves one dataset's record to a different dataset, which is a wrong answer and not a slow
 * path. Non-cryptographic (Murmur3), matching the file-set and listing-cache precedents - this guards
 * accidental collision, not an adversary.
 * <p>
 * What is deliberately NOT here is the connector configuration. It is resolve OUTPUT - a Flight endpoint
 * the resolve discovered, which the query config does not carry - so it is a value, and a key that
 * carried it would both make equality a deep map comparison and put payload in an address. It stays on
 * the stored record, shared by instance across the files of one dataset.
 */
public record SourceScope(String producer, long participantsHigh, long participantsLow) {

    /**
     * @param producer          the registry format name ({@code parquet}, {@code csv}) the resolved metadata reports
     * @param participants      the folded identities of whoever decides what a record here holds: what the storage
     *                          provider says identifies the object, what the format reader says identifies its own
     *                          configuration, and what the coordinator says identifies its own
     * @param definitionVersion the version of the stored definitions this query reads under. Folded in alongside the
     *                          participants rather than kept as a field because it is compared and never read:
     *                          it is not an option a reader parses, so it is not part of what a participant reports
     *                          as its own identity, but it discriminates addresses exactly as those do.
     */
    public static SourceScope of(@Nullable String producer, @Nullable String participants, @Nullable String definitionVersion) {
        StringBuilder encoded = new StringBuilder();
        appendLengthPrefixed(encoded, participants);
        appendLengthPrefixed(encoded, definitionVersion);
        byte[] bytes = encoded.toString().getBytes(StandardCharsets.UTF_8);
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(bytes, 0, bytes.length, 0, new MurmurHash3.Hash128());
        return new SourceScope(producer == null ? "" : producer, hash.h1, hash.h2);
    }

    /** The definition version a config reads under, or {@code ""} for a query with no registered dataset behind it. */
    public static String definitionVersionOf(@Nullable Map<String, Object> config) {
        if (config == null) {
            return "";
        }
        Object version = config.get(DefinitionVersion.CONFIG_KEY);
        return version instanceof String s ? s : "";
    }

    /**
     * Length-prefixed so no participant's value can forge a field boundary: the identities folded in here are open
     * vocabulary, so a plain join would let two different scopes encode identically and share one address.
     */
    private static void appendLengthPrefixed(StringBuilder out, @Nullable String value) {
        if (value == null) {
            out.append("-1:");
        } else {
            out.append(value.length()).append(':').append(value);
        }
    }
}
