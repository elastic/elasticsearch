/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Identifies a backing index that is stalled in the DLM frozen-tier transition, along with the reference timestamp
 * used to detect the stall.
 *
 * <p>For <em>marked</em> indices (those already submitted to or pending on the transition executor),
 * {@code stalledSinceMillis} is {@code max(eligibleSince, masterTenureStart)}: it resets forward on every master
 * failover so that a freshly-elected master does not immediately report an index as stalled before it has had a
 * threshold's worth of time to attempt the transition. This means the value will <em>not</em> match the index's
 * {@code index.lifecycle.origination_date}.
 *
 * <p>For <em>eligible-but-unmarked</em> indices (those past their {@code frozen_after} age but not yet marked),
 * {@code stalledSinceMillis} is the plain eligibility timestamp ({@code generationLifecycleDate + frozenAfter}).
 */
public record DlmFrozenTransitionIndexInfo(ProjectId projectId, String indexName, long stalledSinceMillis) implements Writeable {

    public DlmFrozenTransitionIndexInfo(StreamInput in) throws IOException {
        this(ProjectId.readFrom(in), in.readString(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        projectId.writeTo(out);
        out.writeString(indexName);
        out.writeVLong(stalledSinceMillis);
    }
}
