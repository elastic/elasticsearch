/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * This class represents a node's view of the history of which nodes have been elected master over the last 30 minutes. It is kept in
 * memory, so when a node comes up it does not have any knowledge of previous master history before that point. This object is updated
 * if and when the cluster state changes with a new master node.
 */
public class MasterHistory implements ClusterStateListener {
    private volatile List<TimeAndMaster> masterHistory;
    private LongSupplier currentTimeMillisSupplier;
    /**
     * The maximum amount of time that the master history covers.
     */
    public static final TimeValue MAX_HISTORY_AGE = new TimeValue(30, TimeUnit.MINUTES);

    public MasterHistory(ThreadPool threadPool, ClusterService clusterService) {
        this.masterHistory = new ArrayList<>();
        this.currentTimeMillisSupplier = threadPool::relativeTimeInMillis;
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        DiscoveryNode currentMaster = event.state().nodes().getMasterNode();
        DiscoveryNode previousMaster = event.previousState().nodes().getMasterNode();
        if (currentMaster == null || currentMaster.equals(previousMaster) == false || masterHistory.isEmpty()) {
            long now = currentTimeMillisSupplier.getAsLong();
            long oldestRelevantHistoryTime = now - MAX_HISTORY_AGE.getMillis();
            List<TimeAndMaster> newMasterHistory = masterHistory.stream()
                .filter(timeAndMaster -> timeAndMaster.time >= oldestRelevantHistoryTime)
                .collect(Collectors.toList());
            newMasterHistory.add(new TimeAndMaster(currentTimeMillisSupplier.getAsLong(), currentMaster));
            masterHistory = Collections.unmodifiableList(newMasterHistory);
        }
    }

    /**
     * Returns the node that has been most recently seen as the master
     * @return The node that has been most recently seen as the master, which could be null if no master exists
     */
    public @Nullable DiscoveryNode getMostRecentMaster() {
        List<TimeAndMaster> masterHistoryCopy = getRecentMasterHistory(masterHistory);
        return masterHistoryCopy.isEmpty() ? null : masterHistoryCopy.get(masterHistoryCopy.size() - 1).master;
    }

    /**
     * Returns the most recent non-null master seen, or null if there has been no master seen. Only 30 minutes of history is kept. If the
     * most recent master change is more than 30 minutes old and that change was to set the master to null, then null will be returned.
     * @return The most recent non-null master seen, or null if there has been no master seen.
     */
    public @Nullable DiscoveryNode getMostRecentNonNullMaster() {
        List<TimeAndMaster> masterHistoryCopy = getRecentMasterHistory(masterHistory);
        Collections.reverse(masterHistoryCopy);
        for (TimeAndMaster timeAndMaster : masterHistoryCopy) {
            if (timeAndMaster.master != null) {
                return timeAndMaster.master;
            }
        }
        return null;
    }

    /**
     * Returns true if for the life of this MasterHistory (30 minutes) non-null masters have transitioned to null n times.
     * @param n The number of times a non-null master must have switched to null
     * @return True if non-null masters have transitioned to null n or more times.
     */
    public boolean hasMasterGoneNullAtLeastNTimes(int n) {
        return hasMasterGoneNullAtLeastNTimes(getNodes(), n);
    }

    /**
     * Returns true if for the List of master nodes passed in, non-null masters have transitioned to null n times.
     * So for example:
     * node1 -> null is 1 transition to null
     * node1 -> null -> null is 1 transition to null
     * null -> node1 -> null is 1 transition to null
     * node1 -> null -> node1 is 1 transition to null
     * node1 -> null -> node1 -> null is 2 transitions to null
     * node1 -> null -> node2 -> null is 2 transitions to null
     * @param masters The List of masters to use
     * @param n The number of times a non-null master must have switched to null
     * @return True if non-null masters have transitioned to null n or more timesin the given list of masters.
     */
    public static boolean hasMasterGoneNullAtLeastNTimes(List<DiscoveryNode> masters, int n) {
        int timesMasterHasGoneNull = 0;
        boolean previousNull = true;
        for (DiscoveryNode master : masters) {
            if (master == null) {
                if (previousNull == false) {
                    timesMasterHasGoneNull++;
                }
                previousNull = true;
            } else {
                previousNull = false;
            }
        }
        return timesMasterHasGoneNull >= n;
    }

    /**
     * An identity change is when we get notified of a change to a non-null master that is different from the previous non-null master.
     * So for example:
     * node1 -> node2 is 1 identity change
     * node1 -> node2 -> node1 is 2 identity changes
     * node1 -> node2 -> node2 is 1 identity change (transitions from a node to itself do not count)
     * node1 -> null -> node1 is 0 identity changes (transitions from a node to itself, even with null in the middle, do not count)
     * node1 -> null -> node2 is 1 identity change
     * @param masterHistory The list of nodes that have been master
     * @return The number of master identity changes as defined above
     */
    public static int getNumberOfMasterIdentityChanges(List<DiscoveryNode> masterHistory) {
        int identityChanges = 0;
        List<DiscoveryNode> nonNullHistory = masterHistory.stream().filter(Objects::nonNull).toList();
        DiscoveryNode previousNode = null;
        for (DiscoveryNode node : nonNullHistory) {
            if (previousNode != null && previousNode.equals(node) == false) {
                identityChanges++;
            }
            previousNode = node;
        }
        return identityChanges;
    }

    /**
     * Returns true if a non-null master was seen at any point in the last n seconds, or if the last-seen master was more than n seconds
     * ago and non-null.
     * @param n The number of seconds to look back
     * @return true if the current master is non-null or if a non-null master was seen in the last n seconds
     */
    public boolean hasSeenMasterInLastNSeconds(int n) {
        List<TimeAndMaster> masterHistoryCopy = getRecentMasterHistory(masterHistory);
        long now = currentTimeMillisSupplier.getAsLong();
        TimeValue nSeconds = new TimeValue(n, TimeUnit.SECONDS);
        long nSecondsAgo = now - nSeconds.getMillis();
        return getMostRecentMaster() != null
            || masterHistoryCopy.stream()
                .filter(timeAndMaster -> timeAndMaster.master != null)
                .anyMatch(timeAndMaster -> timeAndMaster.time > nSecondsAgo);
    }

    /*
     * This method creates a copy of masterHistory that only has entries from more than MAX_HISTORY_AGE before now (but leaves the newest
     * entry in even if it is more than MAX_HISTORY_AGE).
     */
    private List<TimeAndMaster> getRecentMasterHistory(List<TimeAndMaster> history) {
        if (history.size() < 2) {
            return history;
        }
        long now = currentTimeMillisSupplier.getAsLong();
        long oldestRelevantHistoryTime = now - MAX_HISTORY_AGE.getMillis();
        TimeAndMaster mostRecent = history.isEmpty() ? null : history.get(history.size() - 1);
        List<TimeAndMaster> filteredHistory = history.stream()
            .filter(timeAndMaster -> timeAndMaster.time > oldestRelevantHistoryTime)
            .collect(Collectors.toList());
        if (filteredHistory.isEmpty() && mostRecent != null) { // The most recent entry was more than 30 minutes ago
            filteredHistory.add(mostRecent);
        }
        return filteredHistory;
    }

    /**
     * This method returns an immutable view of this master history, typically for sending over the wire to another node. The returned List
     * is ordered by when the master was seen, with the earliest-seen masters being first. The List can contain null values. Times are
     * intentionally not included because they cannot be compared across machines.
     * @return An immutable view of this master history
     */
    public List<DiscoveryNode> getNodes() {
        List<TimeAndMaster> masterHistoryCopy = getRecentMasterHistory(masterHistory);
        return masterHistoryCopy.stream().map(TimeAndMaster::master).toList();
    }

    private record TimeAndMaster(long time, DiscoveryNode master) {}
}
