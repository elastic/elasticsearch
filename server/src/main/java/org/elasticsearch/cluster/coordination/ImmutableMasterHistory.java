/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class represents a node's view of the history of which nodes have been elected master over the last 30 minutes. It is kept in
 * memory, so when a node comes up it does not have any knowledge of previous master history before that point. This MasterHistory
 * implementation is immutable -- it is a snapshot in time and is never updated.
 */
public class ImmutableMasterHistory implements MasterHistory, Writeable, Writeable.Reader<ImmutableMasterHistory> {
    private final List<DiscoveryNode> masterHistory;

    public ImmutableMasterHistory(List<DiscoveryNode> masterHistory) {
        this.masterHistory = Collections.unmodifiableList(masterHistory);
    }

    public ImmutableMasterHistory(StreamInput streamInput) throws IOException {
        int mastersCount = streamInput.readVInt();
        List<DiscoveryNode> masters = new ArrayList<>(mastersCount);
        for (int i = 0; i < mastersCount; i++) {
            masters.add(streamInput.readOptionalWriteable(DiscoveryNode::new));
        }
        masterHistory = Collections.unmodifiableList(masters);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(masterHistory.size());
        for (DiscoveryNode master : masterHistory) {
            out.writeOptionalWriteable(master);
        }
    }

    @Override
    public ImmutableMasterHistory read(StreamInput in) throws IOException {
        return new ImmutableMasterHistory(in);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        return other instanceof ImmutableMasterHistory && masterHistory.equals(((ImmutableMasterHistory) other).masterHistory);
    }

    @Override
    public int hashCode() {
        return masterHistory.hashCode();
    }

    @Override
    public DiscoveryNode getCurrentMaster() {
        if (masterHistory == null || masterHistory.isEmpty()) {
            return null;
        }
        return masterHistory.get(masterHistory.size() - 1);
    }

    @Override
    public DiscoveryNode getMostRecentNonNullMaster() {
        DiscoveryNode mostRecentNonNullMaster = null;
        for (int i = masterHistory.size() - 1; i >= 0; i--) {
            DiscoveryNode master = masterHistory.get(i);
            if (master != null) {
                mostRecentNonNullMaster = master;
                break;
            }
        }
        return mostRecentNonNullMaster;
    }

    @Override
    public boolean hasSameMasterGoneNullNTimes(int n) {
        if (getDistinctMastersSeen().size() != 1) {
            return false;
        }
        boolean seenNonNull = false;
        int timesMasterHasGoneNull = 0;
        for (DiscoveryNode master : masterHistory) {
            if (master != null) {
                seenNonNull = true;
            } else if (seenNonNull) {
                timesMasterHasGoneNull++;
            }
        }
        return timesMasterHasGoneNull >= n;
    }

    @Override
    public Set<DiscoveryNode> getDistinctMastersSeen() {
        return masterHistory.stream().filter(Objects::nonNull).collect(Collectors.toSet());
    }
}
