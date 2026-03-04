/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

/**
 * Unique byte identifiers for pipeline stages, persisted in the encoded data.
 *
 * <p>Stage IDs are the wire format contract between encoder and decoder. Once assigned,
 * an ID must <strong>never</strong> change or be reused, as doing so would break the
 * ability to decode existing data. ID {@code 0x00} is reserved for test-only stages
 * (see {@code TestPayloadCodecStage}).
 *
 * <p>Transform stages (IDs below {@code 0xA1}) are applied in sequence and are reversible.
 * Terminal payload stages (IDs at or above {@code 0xA1}) serialize values to bytes as the
 * final step of the encode pipeline. ID {@code 0xA0} is reserved for test-only payload stages.
 *
 */
// NOTE: This enum declares the complete set of stage IDs for the pipeline codec.
// Concrete stage implementations are introduced in subsequent PRs; the IDs are declared
// here upfront because they are the wire format contract - adding them now locks in the
// byte assignments and ensures the registry is self-consistent from day one.
// A subset (e.g., DELTA_STAGE, OFFSET_STAGE, GCD_STAGE, BITPACK_PAYLOAD) is used by
// framework-level tests to exercise the pipeline without depending on real stage code.
public enum StageId {
    DELTA_STAGE((byte) 0x01, "delta"),
    OFFSET_STAGE((byte) 0x02, "offset"),
    GCD_STAGE((byte) 0x03, "gcd"),
    PATCHED_PFOR_STAGE((byte) 0x04, "pfor"),
    XOR_STAGE((byte) 0x05, "xor"),
    ALP_DOUBLE_STAGE((byte) 0x06, "alpDouble"),
    ALP_FLOAT_STAGE((byte) 0x07, "alpFloat"),
    FPC_DOUBLE_STAGE((byte) 0x08, "fpcDouble"),
    FPC_FLOAT_STAGE((byte) 0x09, "fpcFloat"),

    BITPACK_PAYLOAD((byte) 0xA1, "bitPack"),
    ZSTD_PAYLOAD((byte) 0xA2, "zstd"),
    LZ4_PAYLOAD((byte) 0xA3, "lz4"),
    GORILLA_DOUBLE_PAYLOAD((byte) 0xA4, "gorillaDouble"),
    GORILLA_FLOAT_PAYLOAD((byte) 0xA5, "gorillaFloat"),
    CHIMP_DOUBLE_PAYLOAD((byte) 0xA6, "chimpDouble"),
    CHIMP_FLOAT_PAYLOAD((byte) 0xA7, "chimpFloat"),
    CHIMP128_DOUBLE_PAYLOAD((byte) 0xA8, "chimp128Double"),
    CHIMP128_FLOAT_PAYLOAD((byte) 0xA9, "chimp128Float");

    /** Persisted byte identifier. Must never change once assigned. */
    public final byte id;

    /** Human-readable name for logging and diagnostics. */
    public final String displayName;

    /**
     * @param id          the persisted byte identifier
     * @param displayName the human-readable name for logging
     */
    StageId(byte id, final String displayName) {
        this.id = id;
        this.displayName = displayName;
    }

    /**
     * Resolves a persisted byte identifier back to its {@link StageId} constant.
     *
     * @param id the byte identifier read from the encoded data
     * @return the corresponding {@link StageId}
     * @throws IllegalArgumentException if the identifier is unknown
     */
    public static StageId fromId(byte id) {
        return switch (id) {
            case (byte) 0x01 -> DELTA_STAGE;
            case (byte) 0x02 -> OFFSET_STAGE;
            case (byte) 0x03 -> GCD_STAGE;
            case (byte) 0x04 -> PATCHED_PFOR_STAGE;
            case (byte) 0x05 -> XOR_STAGE;
            case (byte) 0x06 -> ALP_DOUBLE_STAGE;
            case (byte) 0x07 -> ALP_FLOAT_STAGE;
            case (byte) 0x08 -> FPC_DOUBLE_STAGE;
            case (byte) 0x09 -> FPC_FLOAT_STAGE;
            case (byte) 0xA1 -> BITPACK_PAYLOAD;
            case (byte) 0xA2 -> ZSTD_PAYLOAD;
            case (byte) 0xA3 -> LZ4_PAYLOAD;
            case (byte) 0xA4 -> GORILLA_DOUBLE_PAYLOAD;
            case (byte) 0xA5 -> GORILLA_FLOAT_PAYLOAD;
            case (byte) 0xA6 -> CHIMP_DOUBLE_PAYLOAD;
            case (byte) 0xA7 -> CHIMP_FLOAT_PAYLOAD;
            case (byte) 0xA8 -> CHIMP128_DOUBLE_PAYLOAD;
            case (byte) 0xA9 -> CHIMP128_FLOAT_PAYLOAD;
            default -> throw new IllegalArgumentException("Unknown stage ID: 0x" + Integer.toHexString(id & 0xFF));
        };
    }
}
