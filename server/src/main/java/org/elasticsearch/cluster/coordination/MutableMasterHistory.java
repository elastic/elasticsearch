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
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class represents a node's view of the history of which nodes have been elected master over the last 30 minutes. It is kept in
 * memory, so when a node comes up it does not have any knowledge of previous master history before that point. This object is updated
 * if and when the cluster state changes with a new master node.
 */
public class MutableMasterHistory implements MasterHistory, ClusterStateListener {
    volatile List<TimeAndMaster> masterHistory;
    private final Object mutex;
    Supplier<Long> nowSupplier; // Can be changed for testing

    public MutableMasterHistory(ThreadPool threadPool, ClusterService clusterService) {
        this.masterHistory = new ArrayList<>();
        this.mutex = new Object();
        this.nowSupplier = threadPool::relativeTimeInMillis;
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        DiscoveryNode currentMaster = event.state().nodes().getMasterNode();
        DiscoveryNode previousMaster = event.previousState().nodes().getMasterNode();
        if (currentMaster == null || currentMaster.equals(previousMaster) == false || masterHistory.isEmpty()) {
            masterHistory.add(new TimeAndMaster(nowSupplier.get(), currentMaster));
            removeOldMasterHistory();
        }
    }

    @Override
    public @Nullable DiscoveryNode getCurrentMaster() {
        return masterHistory.isEmpty() ? null : masterHistory.get(masterHistory.size() - 1).master;
    }

    @Override
    public @Nullable DiscoveryNode getMostRecentNonNullMaster() {
        removeOldMasterHistory();
        DiscoveryNode mostRecentNonNullMaster = null;
        for (int i = masterHistory.size() - 1; i >= 0; i--) {
            TimeAndMaster timeAndMaster = masterHistory.get(i);
            if (timeAndMaster.master != null) {
                mostRecentNonNullMaster = timeAndMaster.master;
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
        for (TimeAndMaster timeAndMaster : masterHistory) {
            if (timeAndMaster.master != null) {
                seenNonNull = true;
            } else if (seenNonNull) {
                timesMasterHasGoneNull++;
            }
        }
        return timesMasterHasGoneNull >= n;
    }

    @Override
    public Set<DiscoveryNode> getDistinctMastersSeen() {
        removeOldMasterHistory();
        return masterHistory.stream()
            .filter(timeAndMaster -> timeAndMaster.master != null)
            .map(timeAndMaster -> timeAndMaster.master)
            .collect(Collectors.toSet());
    }

    /**
     * Returns true if a non-null master was seen at any point in the last n seconds, or if the last-seen master was more than n seconds
     * ago and non-null.
     * @param n The number of seconds to look back
     * @return true if the current master is non-null or if a non-null master was seen in the last n seconds
     */
    public boolean hasSeenMasterInLastNSeconds(int n) {
        long now = nowSupplier.get();
        TimeValue nSeconds = new TimeValue(n, TimeUnit.SECONDS);
        long nSecondsAgo = now - nSeconds.getMillis();
        return getCurrentMaster() != null
            || masterHistory.stream().anyMatch(timeAndMaster -> timeAndMaster.time > nSecondsAgo && timeAndMaster.master != null);
    }

    /**
     * Clears out anything from masterHistory that is from more than 30 minutes before now (but leaves the newest entry in even if it is
     * more than 30 minutes old). Rather than being scheduled, this method is called from other methods in this class as needed
     */
    private void removeOldMasterHistory() {
        synchronized (mutex) {
            if (masterHistory.size() < 2) {
                return;
            }
            long now = nowSupplier.get();
            TimeValue thirtyMinutes = new TimeValue(30, TimeUnit.MINUTES);
            long thirtyMinutesAgo = now - thirtyMinutes.getMillis();
            TimeAndMaster mostRecent = masterHistory.isEmpty() ? null : masterHistory.get(masterHistory.size() - 1);
            masterHistory = masterHistory.stream()
                .filter(timeAndMaster -> timeAndMaster.time > thirtyMinutesAgo)
                .collect(Collectors.toList());
            if (masterHistory.isEmpty() && mostRecent != null) { // The most recent entry was more than 30 minutes ago
                masterHistory.add(mostRecent);
            }
        }
    }

    /**
     * This method returns an immutable view of this master history, typically for sending over the wire to another node.
     * @return An immutable view of this master history
     */
    public ImmutableMasterHistory getImmutableView() {
        removeOldMasterHistory();
        return new ImmutableMasterHistory(masterHistory.stream().map(timeAndMaster -> timeAndMaster.master).collect(Collectors.toList()));
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        return other instanceof MutableMasterHistory && masterHistory.equals(((MutableMasterHistory) other).masterHistory);
    }

    @Override
    public int hashCode() {
        return masterHistory.hashCode();
    }

    private static class TimeAndMaster {
        private final long time;
        private final DiscoveryNode master;

        TimeAndMaster(long time, DiscoveryNode master) {
            this.time = time;
            this.master = master;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (other instanceof TimeAndMaster == false) {
                return false;
            }
            TimeAndMaster otherTimeAndMaster = (TimeAndMaster) other;
            return time == otherTimeAndMaster.time
                && ((master == null && otherTimeAndMaster.master == null) || (master != null && master.equals(otherTimeAndMaster.master)));
        }

        @Override
        public int hashCode() {
            return Objects.hash(time, master);
        }

    }
}
