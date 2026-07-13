/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.charset.StandardCharsets;

/**
 * Per-page composition of the {@code _id} metadata column for external datasets. The composed
 * value is opaque: {@code base64url(murmur3_128(location).h1 | mtime | rowPosition)} — 24
 * identity bytes rendered as a fixed {@value #RENDERED_LENGTH}-character URL-safe string. The
 * location (storage path) is hashed, never rendered: file URIs are an implementation detail of
 * the dataset layout and must not leak through {@code _id} into Kibana row keys, Security alert
 * hashes, or logs. Same composition as the house precedent for synthesized ids,
 * {@code TsidExtractingIdFieldMapper#createId}: murmur3-128 the high-entropy variable part,
 * pack the scalar parts alongside, base64url-encode without padding. Only the high 64 bits
 * ({@code h1}) of the 128-bit location hash are packed, so cross-file {@code _id} uniqueness is
 * probabilistic at 64 bits of location entropy (plus mtime and row position) — the same
 * birthday-bound trade-off the TSID precedent accepts, not a guarantee.
 * <p>
 * The packed mtime is the identity salt: a file replaced in place under the same name produces
 * ids distinct from its predecessor's — without it, a consumer caching by {@code _id} would
 * silently conflate rows from two different file generations. {@code mtime == 0} is the
 * {@link FileList} convention for "storage layer reported none" and packs as zero; {@code _id}
 * stays well-formed and the honest unknown surfaces through {@code _version}'s null.
 * <p>
 * The row position is masked off the optional {@link ColumnExtractor#LOCAL_POSITION_BITS}-encoded
 * extractor id used by the deferred-extraction path before it enters the identity bytes, so the
 * same physical row composes the same {@code _id} regardless of extraction strategy.
 * <p>
 * Allocation discipline: one 16-byte {@link BytesRef} per file for the identity prefix; one
 * exactly-sized {@code byte[]} plus one {@code int[]} of offsets per page; per-row packing and
 * rendering run on two stack-resident scratch buffers, zero per-row allocation.
 */
public final class ExternalRowIdentity {

    /**
     * Mask covering only the per-extractor physical row identity bits, used to strip any encoded
     * extractor id off a {@code _rowPosition} value before it enters the identity bytes. The
     * deferred-extraction path emits encoded {@code (id << LOCAL_POSITION_BITS) | physical}
     * values; only the physical part is row identity.
     */
    static final long LOCAL_POSITION_MASK = (1L << ColumnExtractor.LOCAL_POSITION_BITS) - 1L;

    /** Same seed as {@code TsidExtractingIdFieldMapper}. */
    private static final long SEED = 0;

    /** Per-file prefix bytes: location-hash (8) + mtime (8). */
    static final int PREFIX_BYTES = 16;

    /** Identity bytes per row: {@link #PREFIX_BYTES} + row position (8). */
    static final int IDENTITY_BYTES = PREFIX_BYTES + Long.BYTES;

    /**
     * Rendered length: base64url of {@value #IDENTITY_BYTES} bytes — no padding because
     * {@code IDENTITY_BYTES % 3 == 0} — is exactly 32 characters.
     */
    public static final int RENDERED_LENGTH = (IDENTITY_BYTES / 3) * 4;

    private ExternalRowIdentity() {}

    /**
     * Build the per-file identity prefix: {@code [murmur3_128(locationUtf8).h1 BE | mtimeMillis BE]},
     * {@value #PREFIX_BYTES} bytes. One allocation per file; the returned {@link BytesRef} is held
     * by the producer iterator for the lifetime of the file and reused across every page. Only the
     * first 8 hash bytes are kept — the truncation {@code TsidExtractingIdFieldMapper} applies to
     * the tsid hash; the packed mtime and row position keep same-file rows distinct regardless.
     * {@code mtimeMillis} is the file's last-modified epoch millis; callers pass {@code 0} when
     * the storage layer reports none (the {@link FileList} convention for a missing mtime).
     */
    public static BytesRef prefix(StoragePath path, long mtimeMillis) {
        byte[] location = path.toString().getBytes(StandardCharsets.UTF_8);
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(location, 0, location.length, SEED, new MurmurHash3.Hash128());
        byte[] buf = new byte[PREFIX_BYTES];
        ByteUtils.writeLongBE(hash.h1, buf, 0);
        ByteUtils.writeLongBE(mtimeMillis, buf, 8);
        return new BytesRef(buf, 0, buf.length);
    }

    /**
     * Compose an {@code _id} block for one page. Output position count matches
     * {@code rowPositionBlock.getPositionCount()}; null row-positions yield null {@code _id}. The
     * block allocates against {@code factory} so its breaker bytes follow the producer-thread
     * accounting path used by other constant-block allocations. Two-pass design: pass 1 walks the
     * row-position block, packs {@code prefix + physicalPosition} into the identity scratch and
     * base64url-renders each row directly into a single producer-side {@code backing} array while
     * recording per-row offsets; pass 2 walks {@code offsets[]} and feeds an
     * {@code (offset, length)} scratch view per row to the vector / block builder.
     */
    public static BytesRefBlock composePage(BytesRef prefix, LongBlock rowPositionBlock, BlockFactory factory) {
        int positions = rowPositionBlock.getPositionCount();
        if (positions == 0) {
            return (BytesRefBlock) factory.newConstantNullBlock(0);
        }
        // Exact sizing: every non-null row renders exactly RENDERED_LENGTH bytes. multiplyExact
        // surfaces a pathological page size as ArithmeticException in production rather than as
        // a silent int overflow downstream.
        int backingSize = Math.multiplyExact(positions, RENDERED_LENGTH);
        // The rendering scratch (`backing` + `offsets`) is plain heap, so reserve it against the
        // breaker for its lifetime — at high parallelism the per-page bytes add up to real memory
        // that must not bypass accounting. The reservation is released after the builders below
        // copy the bytes into their own (breaker-accounted) storage.
        long scratchBytes = backingSize + (long) (positions + 1) * Integer.BYTES;
        factory.adjustBreaker(scratchBytes);
        try {
            byte[] backing = new byte[backingSize];
            int[] offsets = new int[positions + 1];
            byte[] identity = new byte[IDENTITY_BYTES];
            byte[] rendered = new byte[RENDERED_LENGTH];
            System.arraycopy(prefix.bytes, prefix.offset, identity, 0, PREFIX_BYTES);
            int cursor = 0;
            boolean anyNull = false;
            for (int i = 0; i < positions; i++) {
                offsets[i] = cursor;
                if (rowPositionBlock.isNull(i)) {
                    anyNull = true;
                    continue;
                }
                int valueIdx = rowPositionBlock.getFirstValueIndex(i);
                long encoded = rowPositionBlock.getLong(valueIdx);
                long physical = encoded & LOCAL_POSITION_MASK;
                ByteUtils.writeLongBE(physical, identity, PREFIX_BYTES);
                Strings.BASE_64_NO_PADDING_URL_ENCODER.encode(identity, rendered);
                System.arraycopy(rendered, 0, backing, cursor, RENDERED_LENGTH);
                cursor += RENDERED_LENGTH;
            }
            offsets[positions] = cursor;

            if (anyNull) {
                // Mixed null/non-null: fall back to BytesRefBlock.Builder so per-row null bitmap is
                // recorded. Still single-page allocation; the temporary byte arrays we already built
                // are reused as the input bytes per row.
                try (BytesRefBlock.Builder builder = factory.newBytesRefBlockBuilder(positions)) {
                    BytesRef scratch = new BytesRef();
                    scratch.bytes = backing;
                    for (int i = 0; i < positions; i++) {
                        if (rowPositionBlock.isNull(i)) {
                            builder.appendNull();
                        } else {
                            scratch.offset = offsets[i];
                            scratch.length = offsets[i + 1] - offsets[i];
                            builder.appendBytesRef(scratch);
                        }
                    }
                    return builder.build();
                }
            }

            // Dense path: feed every row to the vector builder via a scratch view over the producer
            // backing array. The builder copies each appended BytesRef into its own internal buffer,
            // so the rendered vector is not literally backed by `backing` — the producer-side
            // allocations here are the rendering scratch (`backing` + `offsets`), not the vector's
            // storage.
            try (BytesRefVector.Builder vectorBuilder = factory.newBytesRefVectorBuilder(positions)) {
                BytesRef scratch = new BytesRef();
                scratch.bytes = backing;
                for (int i = 0; i < positions; i++) {
                    scratch.offset = offsets[i];
                    scratch.length = offsets[i + 1] - offsets[i];
                    vectorBuilder.appendBytesRef(scratch);
                }
                return vectorBuilder.build().asBlock();
            }
        } finally {
            factory.adjustBreaker(-scratchBytes);
        }
    }
}
