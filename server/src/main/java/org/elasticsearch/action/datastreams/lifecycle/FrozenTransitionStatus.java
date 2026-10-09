/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams.lifecycle;

import java.util.Locale;

/**
 * The status of an index with respect to its data stream lifecycle's frozen tier transition, reported by the
 * lifecycle explain API as the {@code frozen_transition_status} field.
 * <p>
 * {@link #WAITING}, {@link #ELIGIBLE}, and {@link #MARKED} are derived from durable cluster state and are
 * consistent regardless of which node answers the request. {@link #QUEUED} and {@link #RUNNING} are best-effort:
 * they reflect only the in-process state of whichever node is currently the elected master, and reset to
 * {@link #MARKED} across a master failover even for an index whose transition was genuinely in progress on the
 * previous master.
 */
public enum FrozenTransitionStatus {
    /** The index is not yet old enough relative to its data stream's {@code frozen_after} setting. */
    WAITING,
    /** The index is past its {@code frozen_after} age and eligible for transition, but has not been marked yet. */
    ELIGIBLE,
    /** The index has been marked for transition and is waiting to be picked up by the transition executor. */
    MARKED,
    /** The transition has been submitted to the executor and is waiting in its queue. */
    QUEUED,
    /** The transition is actively executing on the current master node. */
    RUNNING,
    /** The lifecycle configures {@code frozen_after}, but no frozen tier transition plugin is installed. */
    NOT_SUPPORTED;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
