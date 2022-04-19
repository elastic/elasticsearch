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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class represents a node's view of the history of which nodes have been elected master over the last 30 minutes. It is kept in
 * memory, so when a node comes up it does not have any knowledge of previous master history before that point.
 */
public class MasterHistory implements ClusterStateListener, Writeable, Writeable.Reader<MasterHistory> {
    private volatile List<TimeAndMaster> masterHistory;
    private final Object mutex;
    Supplier<Long> nowSupplier = System::currentTimeMillis; // Can be changed for testing

    public MasterHistory(ClusterService clusterService) {
        this.masterHistory = new ArrayList<>();
        this.mutex = new Object();
        clusterService.addListener(this);
    }

    public MasterHistory(StreamInput streamInput) throws IOException {
        this.masterHistory = streamInput.readList(TimeAndMaster::new);
        this.mutex = new Object();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        DiscoveryNode currentMaster = event.state().nodes().getMasterNode();
        DiscoveryNode previousMaster = event.previousState().nodes().getMasterNode();
        synchronized (mutex) {
            if (currentMaster == null || currentMaster.equals(previousMaster) == false || masterHistory.isEmpty()) {
                masterHistory.add(new TimeAndMaster(nowSupplier.get(), currentMaster));
            }
        }
        removeOldMasterHistory();
    }

    /**
     * Returns the node that has been most recently seen as the master
     * @return The node that has been most recently seen as the master, which could be null if no master exists
     */
    public @Nullable DiscoveryNode getCurrentMaster() {
        return masterHistory.isEmpty() ? null : masterHistory.get(masterHistory.size() - 1).master;
    }

    /**
     * Returns the most recent non-null master seen, or null if there has been no master seen. Only 30 minutes of history is kept. If the
     * most recent master change is more than 30 minutes old and that change was to set the master to null, then null will be returned.
     * @return The most recent non-null master seen, or null if there has been no master seen.
     */
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

    /**
     * Returns true if for the life of this MasterHistory (30 minutes) only one non-null node has been master, and the master has switched
     * from that node to null n times.
     * @param n The number of times the non-null master must have switched to null
     * @return True if there has been a single non-null master and it has switched to null n or more times.
     */
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

    /**
     * Returns the set of distinct non-null master nodes seen in this history.
     * @return The set of all non-null master nodes seen. Could be empty
     */
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(masterHistory);
    }

    @Override
    public MasterHistory read(StreamInput in) throws IOException {
        return new MasterHistory(in);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        return other instanceof MasterHistory && masterHistory.equals(((MasterHistory) other).masterHistory);
    }

    @Override
    public int hashCode() {
        return masterHistory.hashCode();
    }

    private static class TimeAndMaster implements Writeable, Writeable.Reader<TimeAndMaster> {
        private final long time;
        private final DiscoveryNode master;

        TimeAndMaster(long time, DiscoveryNode master) {
            this.time = time;
            this.master = master;
        }

        TimeAndMaster(StreamInput streamInput) throws IOException {
            this(streamInput.readLong(), streamInput.readOptionalWriteable(DiscoveryNode::new));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(time);
            out.writeOptionalWriteable(master);
        }

        @Override
        public TimeAndMaster read(StreamInput in) throws IOException {
            return new TimeAndMaster(in);
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
