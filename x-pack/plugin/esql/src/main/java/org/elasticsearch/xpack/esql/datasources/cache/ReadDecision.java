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

import java.util.List;

/**
 * Which read a statistic was measured under: the resolved read configuration, as the two 64-bit lanes
 * {@link ReadConfigFingerprint} computes rather than the hex text it renders them as.
 * <p>
 * A statistic is a property of a read, not of a file. Two reads of one file that resolved different
 * schemas measured different things and must not share an address, which is why this is a component of
 * every statistics key and of no schema key: a schema describes the file itself and is the same answer
 * whoever asks.
 * <p>
 * Lanes, not hex. {@link ReadConfigFingerprint#of} already hashes to 128 bits and then renders them as
 * thirty-two characters, so a key holding that text carried a String header, a length, a hash field and
 * sixty-four bytes of payload to say what sixteen bytes say, once per file per distinct read, against a
 * statistics budget that is a fraction of {@code esql.external.cache.size}. The hex form survives where
 * it is genuinely text - the {@code _stats.*} metadata entry that rides the wire with a plan fragment -
 * and {@link #toFingerprint()} bridges to it.
 * <p>
 * The two sentinels are their own {@link Kind} rather than reserved lane values. The hex form could
 * argue no real fingerprint collides with {@code ""} or {@code "mixed"} because a real one is always
 * exactly thirty-two characters; two longs have no such spare room, and a reserved pair would be a
 * magic value that a real hash could one day produce. A named kind cannot collide at all.
 */
public record ReadDecision(Kind kind, long high, long low) {

    public enum Kind {
        /** A resolved read configuration, hashed. */
        KNOWN,
        /**
         * No coordinator-minted read schema was available - an older node, or a source that computes no pin.
         * A legitimate state, and never a licence to share: not knowing which read produced a measurement is
         * not knowing that it matches, so an unknown decision safe-misses to a scan.
         */
        UNKNOWN,
        /**
         * A fold over reads of DIFFERING configurations: a {@code union_by_name} glob whose files resolve to
         * per-file-different read schemas, or a mixed stamped/unstamped input set. Distinct from UNKNOWN because
         * the serve gate must treat it as known-and-never-matching and strip, where an absent decision takes the
         * columnar pass-through and would serve the fold to a read no constituent measured.
         */
        MIXED
    }

    public static final ReadDecision UNKNOWN = new ReadDecision(Kind.UNKNOWN, 0L, 0L);
    public static final ReadDecision MIXED = new ReadDecision(Kind.MIXED, 0L, 0L);

    /**
     * The decision one file's read resolves to. {@code readSchema} is the per-file effective schema the reader
     * will bind, in logical names as the resolution produced them; renames are physicalized inside the
     * fingerprinter so both sides agree.
     */
    public static ReadDecision of(@Nullable List<Attribute> readSchema, @Nullable DeclaredReadSpec spec) {
        if (readSchema == null || readSchema.isEmpty()) {
            return UNKNOWN;
        }
        MurmurHash3.Hash128 hash = ReadConfigFingerprint.hash128(readSchema, spec);
        return new ReadDecision(Kind.KNOWN, hash.h1, hash.h2);
    }

    /**
     * Reads a decision back from the hex form carried in a record's {@code _stats.*} metadata, which is where a
     * harvest arriving from a data node reports the read it ran.
     */
    public static ReadDecision fromFingerprint(@Nullable String fingerprint) {
        if (fingerprint == null || ReadConfigFingerprint.UNKNOWN.equals(fingerprint)) {
            return UNKNOWN;
        }
        if (ReadConfigFingerprint.MIXED.equals(fingerprint) || fingerprint.length() != 32) {
            return MIXED;
        }
        try {
            return new ReadDecision(
                Kind.KNOWN,
                Long.parseUnsignedLong(fingerprint.substring(0, 16), 16),
                Long.parseUnsignedLong(fingerprint.substring(16), 16)
            );
        } catch (NumberFormatException e) {
            // Thirty-two characters that are not hex cannot have come from of(); treat as a configuration we
            // cannot name rather than one that matches nothing, since a scan is the safe answer either way.
            return UNKNOWN;
        }
    }

    /** True when this names an actual read, and so may gate a serve. */
    public boolean isKnown() {
        return kind == Kind.KNOWN;
    }

    /** The hex rendering, for the metadata entry that travels with a plan fragment. */
    public String toFingerprint() {
        return switch (kind) {
            case KNOWN -> ReadConfigFingerprint.render(high, low);
            case UNKNOWN -> ReadConfigFingerprint.UNKNOWN;
            case MIXED -> ReadConfigFingerprint.MIXED;
        };
    }
}
