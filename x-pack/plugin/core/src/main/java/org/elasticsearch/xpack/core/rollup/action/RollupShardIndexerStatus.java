/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rollup.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Arrays;

/**
 * Status of the rollup indexer task
 */
public enum RollupShardIndexerStatus implements Writeable {
    INITIALIZED(0),
    STARTED(1),
    FAILED(2),
    COMPLETED(3),
    CANCELLED(4);

    private final byte ordinal;

    RollupShardIndexerStatus(int ordinal) {
        this.ordinal = (byte) ordinal;
    }

    public static RollupShardIndexerStatus readFromStream(final StreamInput in) throws IOException {
        int ordinal = in.readByte();
        return switch (ordinal) {
            case 0 -> RollupShardIndexerStatus.INITIALIZED;
            case 1 -> RollupShardIndexerStatus.STARTED;
            case 2 -> RollupShardIndexerStatus.FAILED;
            case 3 -> RollupShardIndexerStatus.COMPLETED;
            case 4 -> RollupShardIndexerStatus.CANCELLED;
            default -> throw new IllegalArgumentException(
                "Unknown ["
                    + RollupShardIndexerStatus.class.getSimpleName()
                    + "] value ["
                    + ordinal
                    + "]. Expected values ["
                    + Arrays.toString(RollupShardIndexerStatus.values())
                    + "]"
            );
        };
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeByte(this.ordinal);
    }
}
