/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.Nullable;

import java.util.Set;

/**
 * This interface represents a node's view of the history of which nodes have been elected master over the last 30 minutes. It is kept in
 * memory, so when a node comes up it does not have any knowledge of previous master history before that point.
 */
public interface MasterHistory {

    /**
     * Returns the node that has been most recently seen as the master
     * @return The node that has been most recently seen as the master, which could be null if no master exists
     */
    @Nullable
    DiscoveryNode getCurrentMaster();

    /**
     * Returns the most recent non-null master seen, or null if there has been no master seen. Only 30 minutes of history is kept. If the
     * most recent master change is more than 30 minutes old and that change was to set the master to null, then null will be returned.
     * @return The most recent non-null master seen, or null if there has been no master seen.
     */
    @Nullable
    DiscoveryNode getMostRecentNonNullMaster();

    /**
     * Returns true if for the life of this MasterHistory (30 minutes) only one non-null node has been master, and the master has switched
     * from that node to null n times.
     * @param n The number of times the non-null master must have switched to null
     * @return True if there has been a single non-null master and it has switched to null n or more times.
     */
    boolean hasSameMasterGoneNullNTimes(int n);

    /**
     * Returns the set of distinct non-null master nodes seen in this history.
     * @return The set of all non-null master nodes seen. Could be empty
     */
    Set<DiscoveryNode> getDistinctMastersSeen();
}
