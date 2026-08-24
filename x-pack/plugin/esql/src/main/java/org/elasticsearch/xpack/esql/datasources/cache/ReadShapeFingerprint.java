/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.DeclaredReadSpec;
import org.elasticsearch.xpack.esql.datasources.PhysicalNames;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * The 128-bit fingerprint of a file's READ SHAPE — how a query will interpret that file's bytes, as opposed to which
 * file it is ({@code path + mtime}) or which options were asked for ({@link SchemaCacheKey#buildFormatConfig}).
 * <p>
 * It exists because a cached statistic is a measurement over the rows a read produced, and which rows a read produces
 * is a function of the read shape. A dataset declaring a column as {@code long} under {@code skip_row} drops a record
 * that a dataset inferring the same column as {@code keyword} keeps; both address the same file under the same
 * options, so without this component they share one cache entry and serve each other's counts. The same blindness
 * covers a {@code union_by_name} widening pin, where nothing is declared at all and the read type still differs.
 * <p>
 * <b>Derived, never shipped.</b> Both sides compute it from artifacts the coordinator already minted and the wire
 * already carries: the per-file read schema (a split's {@code readSchema}, or the exec's unified schema on the
 * split-less single-file rails) and {@link DeclaredReadSpec}. Nothing new travels between nodes, so this needs no
 * transport version. The corollary is a hard constraint on what may be hashed: only what survives the wire
 * identically on both sides.
 *
 * <h2>What is hashed, and what deliberately is not</h2>
 * <ul>
 *   <li><b>Column name and type, in order</b> — the read schema is the effective (post-overlay) schema, so a declared
 *       retype is already visible in the type, and a positional format's binding IS the order.</li>
 *   <li><b>Per-column declared date pattern</b>, physicalized — it changes which values parse and therefore which
 *       rows survive under a lenient policy.</li>
 *   <li><b>Binding mode</b> — a DECLARED schema binds by name and reports absent columns; an INFERRED one binds by
 *       position. Same columns, different reads.</li>
 *   <li><b>NOT nullability</b> — {@code FileSplit} normalizes the planner-internal UNKNOWN to nullable on the wire,
 *       so a coordinator hashing its in-memory schema and a data node hashing the round-tripped one would disagree.
 *       An identity the two sides compute differently is worse than no identity: it matches nothing, silently.</li>
 *   <li><b>NOT the projection</b> — per-query, and the coordinator resolves the full schema while a data node reads a
 *       subset. Hashing it would fragment the cache per query shape and break cross-node matching.</li>
 *   <li><b>NOT renames</b> — harvested statistic keys are physical on every rail, so encoding logical names would
 *       split a renamed dataset off from its own harvests. Renames are normalized away by physicalizing here; a pure
 *       rename changes no value a statistic measures.</li>
 *   <li><b>NOT {@code declaredTypeColumns}</b> — it licenses a narrowing coercion only in the by-name columnar
 *       readers, and no columnar format stamps statistics. Should one ever start, this exclusion becomes a hole; the
 *       tripwire for that lives in the identity-scope tests.</li>
 * </ul>
 *
 * <h2>Encoding</h2>
 * Every variable-length piece is length-prefixed ({@code len:bytes}). Column names are open vocabulary — an
 * {@code _id.path} rename reaches arbitrary physical names, which may contain the delimiters — so a plain join would
 * let two different shapes render identically and collide onto one cache entry. Equal encodings must genuinely mean
 * equal shapes.
 * <p>
 * 128 bits for the same reason {@link org.elasticsearch.xpack.esql.datasources.FileSetFingerprint} uses 128: a
 * collision serves one read's measurement to a different read — a wrong answer, not a slow path. Non-cryptographic
 * (Murmur3, matching the listing-cache and file-set precedents): this guards accidental collision, not an adversary.
 */
public final class ReadShapeFingerprint {

    private ReadShapeFingerprint() {}

    /**
     * The sentinel for "this read's shape is unknown". Returned when no coordinator-minted read schema is available,
     * which is a legitimate state (an older node, a source that computes no pin). It must never compare equal to a
     * real shape: an unknown shape safe-misses to a scan rather than sharing an entry on the strength of not knowing.
     */
    public static final String UNKNOWN = "";

    /**
     * Computes the fingerprint of one file's read shape. {@code readSchema} is the per-file effective schema the
     * reader will bind, in <b>logical</b> names as the resolution produced them; renames are applied here so both
     * sides agree on a physical-name encoding. Returns {@link #UNKNOWN} when there is no schema to describe.
     */
    public static String of(@Nullable List<Attribute> readSchema, @Nullable DeclaredReadSpec spec) {
        if (readSchema == null || readSchema.isEmpty()) {
            return UNKNOWN;
        }
        DeclaredReadSpec readSpec = spec == null ? DeclaredReadSpec.NONE : spec;
        Map<String, String> renames = readSpec.renames();
        Map<String, String> dateFormats = readSpec.dateFormats();

        StringBuilder encoded = new StringBuilder();
        for (Attribute attribute : readSchema) {
            String logicalName = attribute.name();
            // Physicalize both the name and the date-format lookup with the same mapping the reader boundary uses
            // (FileSourceFactory#physicalDateFormats), so a rename shifts nothing in the encoding.
            appendLengthPrefixed(encoded, PhysicalNames.translate(logicalName, renames));
            appendLengthPrefixed(encoded, attribute.dataType().typeName());
            appendLengthPrefixed(encoded, dateFormats.getOrDefault(logicalName, ""));
        }
        appendLengthPrefixed(encoded, readSpec.provenance().name());

        byte[] bytes = encoded.toString().getBytes(StandardCharsets.UTF_8);
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(bytes, 0, bytes.length, 0, new MurmurHash3.Hash128());
        return Long.toHexString(hash.h1) + Long.toHexString(hash.h2);
    }

    /** Length-prefixed so no user-controlled value can forge a field boundary. */
    private static void appendLengthPrefixed(StringBuilder out, String value) {
        out.append(value.length()).append(':').append(value);
    }
}
