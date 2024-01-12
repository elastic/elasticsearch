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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * This class represents a node's view of the history of which nodes have been elected master over the last 30 minutes. It is kept in
 * memory, so when a node comes up it does not have any knowledge of previous master history before that point. This object is updated
 * if and when the cluster state changes with a new master node.
 */
public class MasterHistory implements ClusterStateListener {
    /**
     * The maximum amount of time that the master history covers.
     */
    private final TimeValue maxHistoryAge;
    private final ClusterService clusterService;
    // Note: While the master can be null, the TimeAndMaster object in this list is never null
    private volatile List<TimeAndMaster> masterHistory;
    private final LongSupplier currentTimeMillisSupplier;
    /**
     * This is the maximum number of master nodes kept in history so that the list doesn't grow extremely large and impact performance if
     * things become really unstable. We don't get additional any value in keeping more than this.
     */
    public static final int MAX_HISTORY_SIZE = 50;

    private static final TimeValue DEFAULT_MAX_HISTORY_AGE = new TimeValue(30, TimeUnit.MINUTES);
    private static final TimeValue SMALLEST_ALLOWED_MAX_HISTORY_AGE = new TimeValue(1, TimeUnit.MINUTES);

    public static final Setting<TimeValue> MAX_HISTORY_AGE_SETTING = Setting.timeSetting(
        "master_history.max_age",
        DEFAULT_MAX_HISTORY_AGE,
        SMALLEST_ALLOWED_MAX_HISTORY_AGE,
        Setting.Property.NodeScope
    );

    @SuppressWarnings("this-escape")
    public MasterHistory(ThreadPool threadPool, ClusterService clusterService) {
        this.masterHistory = new ArrayList<>();
        this.currentTimeMillisSupplier = threadPool.relativeTimeInMillisSupplier();
        this.maxHistoryAge = MAX_HISTORY_AGE_SETTING.get(clusterService.getSettings());
        this.clusterService = clusterService;
        clusterService.addListener(this);
    }

    public TimeValue getMaxHistoryAge() {
        return this.maxHistoryAge;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        DiscoveryNode currentMaster = event.state().nodes().getMasterNode();
        DiscoveryNode previousMaster = event.previousState().nodes().getMasterNode();
        if (currentMaster == null || currentMaster.equals(previousMaster) == false || masterHistory.isEmpty()) {
            long now = currentTimeMillisSupplier.getAsLong();
            long oldestRelevantHistoryTime = now - maxHistoryAge.getMillis();
            List<TimeAndMaster> newMasterHistory = new ArrayList<>();
            int sizeAfterAddingNewMaster = masterHistory.size() + 1;
            int startIndex = Math.max(0, sizeAfterAddingNewMaster - MAX_HISTORY_SIZE);
            for (int i = startIndex; i < masterHistory.size(); i++) {
                TimeAndMaster timeAndMaster = masterHistory.get(i);
                final long currentMasterEndTime;
                if (i < masterHistory.size() - 1) {
                    // We treat the start time of the next master as the end time of this current master
                    currentMasterEndTime = masterHistory.get(i + 1).startTimeMillis;
                } else {
                    currentMasterEndTime = Long.MAX_VALUE; // This current master has no end time
                }
                if (currentMasterEndTime >= oldestRelevantHistoryTime) {
                    newMasterHistory.add(timeAndMaster);
                }
            }
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
     * Note that a master changes to null on (virtually) every identity change.
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
     * Returns true if a non-null master existed at any point in the last nSeconds seconds. Note that this could be a master whose start
     * time was more than nSeconds ago, as long as either it is still master or the next master took over less than nSeconds ago.
     * @param nSeconds The number of seconds to look back
     * @return true if the current master is non-null or if a non-null master was seen in the last nSeconds seconds
     */
    public boolean hasSeenMasterInLastNSeconds(int nSeconds) {
        if (getMostRecentMaster() != null) {
            return true;
        }
        List<TimeAndMaster> masterHistoryCopy = getRecentMasterHistory(masterHistory);
        long now = currentTimeMillisSupplier.getAsLong();
        TimeValue nSecondsTimeValue = new TimeValue(nSeconds, TimeUnit.SECONDS);
        long nSecondsAgo = now - nSecondsTimeValue.getMillis();

        /*
         * We traverse the list backwards (since it is ordered by time ascending). Once we find an entry whose
         * timeAndMaster.startTimeMillis was more than nSeconds ago we can stop because it is not possible that any more of the nodes
         * we'll see have ended within the last nSeconds.
         */
        for (int i = masterHistoryCopy.size() - 1; i >= 0; i--) {
            TimeAndMaster timeAndMaster = masterHistoryCopy.get(i);
            if (timeAndMaster.master != null) {
                return true;
            }
            if (timeAndMaster.startTimeMillis < nSecondsAgo) {
                break;
            }
        }
        return false;
    }

    /*
     * This method creates a mutable copy of masterHistory that only has entries that have been active in the recent past
     * (maxHistoryAge). In this case, "active" means that either the master's start time has been within maxHistoryAge, or the master was
     *  replaced by another master within maxHistoryAge.
     */
    private List<TimeAndMaster> getRecentMasterHistory(List<TimeAndMaster> history) {
        if (history.size() < 2) {
            return history;
        }
        long now = currentTimeMillisSupplier.getAsLong();
        long oldestRelevantHistoryTime = now - maxHistoryAge.getMillis();

        List<TimeAndMaster> filteredHistory = new ArrayList<>();
        for (int i = 0; i < history.size(); i++) {
            TimeAndMaster timeAndMaster = history.get(i);
            final long endTime;
            /*
             * The end time of this timeAndMaster is the start time of the next one in the list. If there is no next one, this is the
             * current timeAndMaster, so there is no endTime (so it is set to Long.MAX_VALUE).
             */
            if (i < history.size() - 1) {
                endTime = history.get(i + 1).startTimeMillis;
            } else {
                endTime = Long.MAX_VALUE;
            }
            if (endTime >= oldestRelevantHistoryTime) {
                filteredHistory.add(timeAndMaster);
            }
        }
        return filteredHistory;
    }

    /**
     * This method returns an immutable view of this master history, typically for sending over the wire to another node. The returned List
     * is ordered by when the master was seen, with the earliest-seen masters being first. The List can contain null values. Times are
     * intentionally not included because they cannot be compared across machines. This list contains nodes even if they are not currently
     * in the cluster.
     * @return An immutable view of this master history
     */
    public List<DiscoveryNode> getRawNodes() {
        List<TimeAndMaster> masterHistoryCopy = getRecentMasterHistory(masterHistory);
        return masterHistoryCopy.stream().map(TimeAndMaster::master).toList();
    }

    /*
     * This method is similar to getRawNodes(), except any non-null nodes whose ephemeral IDs are not in the nodes in the cluster
     * state are removed. This is meant to be used to filter out nodes from the master history that are no longer part of the cluster. We
     * need to keep these nodes in the master history in case they return to the cluster, but we do not want them to count toward our
     * stability calculations.
     */
    public List<DiscoveryNode> getNodes() {
        List<DiscoveryNode> nodes = getRawNodes();
        if (nodes == null || nodes.isEmpty()) {
            return nodes;
        }
        Set<String> ephemeralIdsCurrentlyInCluster = clusterService.state()
            .nodes()
            .stream()
            .map(DiscoveryNode::getEphemeralId)
            .collect(Collectors.toSet());
        return nodes.stream()
            .filter(node -> node == null || ephemeralIdsCurrentlyInCluster.contains(node.getEphemeralId()))
            .collect(Collectors.toList());
    }

    private record TimeAndMaster(long startTimeMillis, DiscoveryNode master) {}
}
