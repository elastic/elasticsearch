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
public enum StageId {
    /** Delta encoding transform stage. */
    DELTA_STAGE((byte) 0x01, "delta"),
    /** Offset removal transform stage. */
    OFFSET_STAGE((byte) 0x02, "offset"),
    /** GCD factoring transform stage. */
    GCD_STAGE((byte) 0x03, "gcd"),

    /** Bit-packing terminal payload stage. */
    BITPACK_PAYLOAD((byte) 0xA1, "bitPack");

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
            case (byte) 0xA1 -> BITPACK_PAYLOAD;
            default -> throw new IllegalArgumentException("Unknown stage ID: 0x" + Integer.toHexString(id & 0xFF));
        };
    }
}
