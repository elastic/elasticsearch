/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.action.datastreams.lifecycle.ExplainIndexFrozenTransition;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.core.Nullable;

/**
 * Extension point implemented by the DLM frozen tier transition plugin to expose the execution status of an index's
 * frozen tier transition to the lifecycle explain API, without introducing a compile-time dependency from this module
 * on that plugin.
 */
public interface FrozenTransitionInfoProvider {

    /**
     * Returns whether a frozen tier transition implementation is actually installed. The lifecycle explain API uses
     * this to decide whether to report frozen tier transition state at all: without an installed implementation, a
     * transition can never run, so reporting a status would be misleading rather than merely "not started yet".
     */
    boolean infoAvailable();

    /**
     * Returns the current execution status of the frozen tier transition for the given index, as tracked by the
     * transition executor, or {@code null} if no implementation is installed (callers must gate on {@link #infoAvailable()}).
     * <p>
     * This status is best-effort: it reflects only the in-process state of whichever node is currently the elected
     * master, and resets to {@link ExplainIndexFrozenTransition.Status#NOT_STARTED} across a master failover, even
     * for an index whose transition was genuinely in progress on the previous master.
     */
    @Nullable
    ExplainIndexFrozenTransition.Status getTransitionStatus(ProjectId projectId, String indexName);

    /**
     * Returns a provider used when no frozen tier transition implementation is installed.
     */
    static FrozenTransitionInfoProvider noop() {
        return new FrozenTransitionInfoProvider() {
            @Override
            public boolean infoAvailable() {
                return false;
            }

            @Override
            public ExplainIndexFrozenTransition.Status getTransitionStatus(ProjectId projectId, String indexName) {
                return null;
            }
        };
    }
}
