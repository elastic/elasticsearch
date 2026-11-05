/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

/**
 * The phases of a downsample operation, in the order the master drives them. Each phase is idempotent, so the master can
 * re-run the current phase after a failover or a failure.
 */
public enum IndexTaskPhase {
    CREATE_TARGET,
    RUN_SHARDS,
    FINALIZE_SETTINGS,
    REFRESH,
    MARK_SUCCESS,
    FLUSH,
    DONE,
    FAILED;

    public boolean isTerminal() {
        return switch (this) {
            case DONE, FAILED -> true;
            case CREATE_TARGET, RUN_SHARDS, FINALIZE_SETTINGS, REFRESH, MARK_SUCCESS, FLUSH -> false;
        };
    }
}
