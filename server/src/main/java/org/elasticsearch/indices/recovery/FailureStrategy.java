/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

/// How a recovery failure should be handled. Chosen by callers of [RecoveryListener#onRecoveryFailure] at failure
/// time and consumed by [org.elasticsearch.indices.cluster.IndicesClusterStateService#handleRecoveryFailure] (except
/// [#ABORT], which is dropped before reaching it). Note that `handleRecoveryFailure` may override the requested
/// strategy when necessary (e.g. convert [#RETRY] into [#FAIL_SEND]).
public enum FailureStrategy {
    RETRY(false),
    ABORT(false),
    FAIL_SILENT(false),

    /**
     * Like {@link #FAIL_SILENT}, the master is not notified straight away, but the allocation is also remembered so that later
     * cluster states which still assign it do not recreate the shard (and so do not start a second recovery for it). If a later
     * cluster state still assigns the allocation, the master has not acted on the failure, so it is sent then, as {@link #FAIL_SEND}
     * would have done.
     */
    FAIL_SILENT_AND_BLOCK(false),
    FAIL_SEND(true);

    private final boolean notifyMaster;

    FailureStrategy(boolean notifyMaster) {
        this.notifyMaster = notifyMaster;
    }

    public boolean notifyMaster() {
        return notifyMaster;
    }
}
