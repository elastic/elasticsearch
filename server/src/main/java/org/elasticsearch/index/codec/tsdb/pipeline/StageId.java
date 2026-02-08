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
// TODO: Consider reserving a range of IDs (e.g., 0x00-0x0F) for future internal use,
// keeping production transformation stages starting at 0x10 and terminal stages at 0xA0.
public enum StageId {
    // Transformation stages (NumericCodecStage) - applied in sequence, reversible
    DELTA((byte) 0x01),
    OFFSET((byte) 0x02),
    GCD((byte) 0x03),
    PATCHED_PFOR((byte) 0x04),
    ZIGZAG((byte) 0x05),

    // Terminal stages (PayloadCodecStage) - write final encoded payload
    BIT_PACK((byte) 0xA1),
    ZSTD((byte) 0xA2);

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
            case (byte) 0x05 -> ZIGZAG;
            case (byte) 0xA1 -> BIT_PACK;
            case (byte) 0xA2 -> ZSTD;
            default -> throw new IllegalArgumentException("Unknown stage ID: 0x" + Integer.toHexString(id & 0xFF));
        };
    }
}
