/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

public enum IndexShardState {
    CREATED((byte) 0),
    RECOVERING((byte) 1),
    POST_RECOVERY((byte) 2),
    STARTED((byte) 3),
    // previously, 4 was the RELOCATED state
    CLOSED((byte) 5);

    private static final IndexShardState[] IDS = new IndexShardState[IndexShardState.values().length + 1]; // +1 for RELOCATED state

    static {
        for (IndexShardState state : IndexShardState.values()) {
            assert state.id() < IDS.length && state.id() >= 0;
            IDS[state.id()] = state;
        }
        assert IDS[4] == null;
        IDS[4] = STARTED; // for backward compatibility reasons (this was the RELOCATED state)
    }

    private final byte id;

    IndexShardState(byte id) {
        this.id = id;
    }

    public byte id() {
        return this.id;
    }

    public static IndexShardState fromId(byte id) {
        if (id < 0 || id >= IDS.length) {
            throw new IllegalArgumentException("No mapping for id [" + id + "]");
        }
        return IDS[id];
    }
}
