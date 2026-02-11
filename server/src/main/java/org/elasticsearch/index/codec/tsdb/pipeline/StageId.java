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
    DELTA((byte) 0x01),
    OFFSET((byte) 0x02),
    GCD((byte) 0x03),
    PATCHED_PFOR((byte) 0x04),
    XOR((byte) 0x06),
    QUANTIZE_DOUBLE((byte) 0x08),
    DELTA_DELTA((byte) 0x05),
    RLE((byte) 0x0B),
    ALP_DOUBLE_STAGE((byte) 0x0C),
    ALP_FLOAT_STAGE((byte) 0x0D),
    ALP_RD_DOUBLE_STAGE((byte) 0x0E),
    ALP_RD_FLOAT_STAGE((byte) 0x0F),
    FPC_STAGE((byte) 0x10),

    // Terminal stages - write final encoded payload
    BIT_PACK((byte) 0xA1),
    ZSTD((byte) 0xA2),
    GORILLA_PAYLOAD((byte) 0xA3),
    RLE_PAYLOAD((byte) 0xA4),
    ALP_DOUBLE((byte) 0xA6),
    ALP_FLOAT((byte) 0xA7),
    ALP_RD_DOUBLE((byte) 0xA8),
    ALP_RD_FLOAT((byte) 0xA9);

    public final byte id;

    StageId(byte id) {
        this.id = id;
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
            case (byte) 0xA1 -> BIT_PACK;
            case (byte) 0xA2 -> ZSTD;
            case (byte) 0xA3 -> GORILLA_PAYLOAD;
            case (byte) 0xA4 -> RLE_PAYLOAD;
            case (byte) 0xA6 -> ALP_DOUBLE;
            case (byte) 0xA7 -> ALP_FLOAT;
            case (byte) 0xA8 -> ALP_RD_DOUBLE;
            case (byte) 0xA9 -> ALP_RD_FLOAT;
            default -> throw new IllegalArgumentException("Unknown stage ID: 0x" + Integer.toHexString(id & 0xFF));
        };
    }
}
