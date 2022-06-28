/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.upgrade.post;

import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to upgrade one or more indices. In order to update all indices, pass an empty array or
 * {@code null} for the indices.
 * @see org.elasticsearch.client.Requests#upgradeRequest(String...)
 * @see org.elasticsearch.client.IndicesAdminClient#upgrade(UpgradeRequest)
 * @see UpgradeResponse
 */
public class UpgradeRequest extends BroadcastRequest<UpgradeRequest> {

    public static final class Defaults {
        public static final boolean UPGRADE_ONLY_ANCIENT_SEGMENTS = false;
    }

    private boolean upgradeOnlyAncientSegments = Defaults.UPGRADE_ONLY_ANCIENT_SEGMENTS;

    /**
     * Constructs an optimization request over one or more indices.
     *
     * @param indices The indices to upgrade, no indices passed means all indices will be optimized.
     */
    public UpgradeRequest(String... indices) {
        super(indices);
    }

    public UpgradeRequest(StreamInput in) throws IOException {
        super(in);
        upgradeOnlyAncientSegments = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(upgradeOnlyAncientSegments);
    }

    /**
     * Should the upgrade only the ancient (older major version of Lucene) segments?
     * Defaults to {@code false}.
     */
    public boolean upgradeOnlyAncientSegments() {
        return upgradeOnlyAncientSegments;
    }

    /**
     * See {@link #upgradeOnlyAncientSegments()}
     */
    public UpgradeRequest upgradeOnlyAncientSegments(boolean upgradeOnlyAncientSegments) {
        this.upgradeOnlyAncientSegments = upgradeOnlyAncientSegments;
        return this;
    }

    @Override
    public String toString() {
        return "UpgradeRequest{" + "upgradeOnlyAncientSegments=" + upgradeOnlyAncientSegments + '}';
    }
}
