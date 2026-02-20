/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

// NOTE: Stage IDs are persisted in the encoded data. For backward compatibility,
// these IDs must NEVER change as it would break the ability to decode existing data.
//
// NOTE: ID 0x00 is reserved for test-only stages (see TestPayloadCodecStage).
public enum StageId {
    // Transformation stages - applied in sequence, reversible
    DELTA((byte) 0x01, "delta"),
    OFFSET((byte) 0x02, "offset"),
    GCD((byte) 0x03, "gcd"),
    PATCHED_PFOR((byte) 0x04, "pfor"),
    XOR((byte) 0x06, "xor"),
    QUANTIZE_DOUBLE((byte) 0x08, "quantize"),
    DELTA_DELTA((byte) 0x05, "deltaDelta"),
    RLE((byte) 0x0B, "rle"),
    ALP_DOUBLE_STAGE((byte) 0x0C, "alp"),
    ALP_FLOAT_STAGE((byte) 0x0D, "alp"),
    ALP_RD_DOUBLE_STAGE((byte) 0x0E, "alpRd"),
    ALP_RD_FLOAT_STAGE((byte) 0x0F, "alpRd"),
    FPC_STAGE((byte) 0x10, "fpc"),
    CHIMP_DOUBLE_STAGE((byte) 0x11, "chimp"),
    CHIMP_FLOAT_STAGE((byte) 0x12, "chimp"),

    // Terminal stages - write final encoded payload
    BIT_PACK((byte) 0xA1, "bitPack"),
    ZSTD((byte) 0xA2, "zstd"),
    GORILLA_PAYLOAD((byte) 0xA3, "gorilla"),
    RLE_PAYLOAD((byte) 0xA4, "rlePayload"),
    ALP_DOUBLE((byte) 0xA6, "alpDouble"),
    ALP_FLOAT((byte) 0xA7, "alpFloat"),
    ALP_RD_DOUBLE((byte) 0xA8, "alpRdDouble"),
    ALP_RD_FLOAT((byte) 0xA9, "alpRdFloat"),
    LZ4((byte) 0xA5, "lz4"),
    GORILLA_FLOAT_PAYLOAD((byte) 0xAA, "gorillaFloat");

    public final byte id;
    public final String displayName;

    StageId(byte id, String displayName) {
        this.id = id;
        this.displayName = displayName;
    }

    public static StageId fromId(byte id) {
        return switch (id) {
            case (byte) 0x01 -> DELTA;
            case (byte) 0x02 -> OFFSET;
            case (byte) 0x03 -> GCD;
            case (byte) 0x04 -> PATCHED_PFOR;
            case (byte) 0x05 -> DELTA_DELTA;
            case (byte) 0x06 -> XOR;
            case (byte) 0x08 -> QUANTIZE_DOUBLE;
            case (byte) 0x0B -> RLE;
            case (byte) 0x0C -> ALP_DOUBLE_STAGE;
            case (byte) 0x0D -> ALP_FLOAT_STAGE;
            case (byte) 0x0E -> ALP_RD_DOUBLE_STAGE;
            case (byte) 0x0F -> ALP_RD_FLOAT_STAGE;
            case (byte) 0x10 -> FPC_STAGE;
            case (byte) 0x11 -> CHIMP_DOUBLE_STAGE;
            case (byte) 0x12 -> CHIMP_FLOAT_STAGE;
            case (byte) 0xA1 -> BIT_PACK;
            case (byte) 0xA2 -> ZSTD;
            case (byte) 0xA3 -> GORILLA_PAYLOAD;
            case (byte) 0xA4 -> RLE_PAYLOAD;
            case (byte) 0xA6 -> ALP_DOUBLE;
            case (byte) 0xA7 -> ALP_FLOAT;
            case (byte) 0xA8 -> ALP_RD_DOUBLE;
            case (byte) 0xA9 -> ALP_RD_FLOAT;
            case (byte) 0xA5 -> LZ4;
            case (byte) 0xAA -> GORILLA_FLOAT_PAYLOAD;
            default -> throw new IllegalArgumentException("Unknown stage ID: 0x" + Integer.toHexString(id & 0xFF));
        };
    }
}
