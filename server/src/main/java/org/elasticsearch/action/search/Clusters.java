/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Holds info about the clusters that the search was executed on: how many in total, how many of them were successful
 * and how many of them were skipped.
 */
public class Clusters implements ToXContentFragment, Writeable {

    public static final Clusters EMPTY = new Clusters(0, 0, 0);

    static final ParseField _CLUSTERS_FIELD = new ParseField("_clusters");
    static final ParseField SUCCESSFUL_FIELD = new ParseField("successful");
    static final ParseField SKIPPED_FIELD = new ParseField("skipped");
    static final ParseField TOTAL_FIELD = new ParseField("total");

    private final int total;
    private final int successful;
    private final int skipped;
    // NOTE: these two new fields (remoteClusters and ccsMinimizeRoundtrips) have not been added to the wire protocol
    // or equals/hashCode methods. They are needed for CCS only (async-search CCS in particular). If we need to write
    // these to the .async-search system index in the future, we may want to refactor Clusters to allow async-search
    // to subclass it.
    private final transient int remoteClusters;
    private final transient boolean ccsMinimizeRoundtrips;

    /**
     * An Clusters object meant for use with CCS holding additional information about
     * the number of remote clusters and whether ccsMinimizeRoundtrips is being used.
     *
     * @param total
     * @param successful
     * @param skipped
     * @param remoteClusters
     * @param ccsMinimizeRoundtrips
     */
    public Clusters(int total, int successful, int skipped, int remoteClusters, boolean ccsMinimizeRoundtrips) {
        assert total >= 0 && successful >= 0 && skipped >= 0 && remoteClusters >= 0
            : "total: " + total + " successful: " + successful + " skipped: " + skipped + " remote: " + remoteClusters;
        assert successful <= total : "total: " + total + " successful: " + successful + " skipped: " + skipped;
        assert remoteClusters <= total : "total: " + total + " remote: " + remoteClusters;
        assert ccsMinimizeRoundtrips == false || remoteClusters > 0
            : "ccsMinimizeRoundtrips is true but remoteClusters count is not a positive number: " + remoteClusters;
        int localCount = total - remoteClusters;
        assert localCount == 0 || localCount == 1 : "total - remoteClusters should only be 0 or 1";
        this.total = total;
        this.successful = successful;
        this.skipped = skipped;
        this.remoteClusters = remoteClusters;
        this.ccsMinimizeRoundtrips = ccsMinimizeRoundtrips;
    }

    /**
     * Creates a Clusters object in its "final" state, where successful + skipped = total.
     * All clusters must be accounted for.
     * @param total total number of clusters being searched
     * @param successful
     * @param skipped
     */
    public Clusters(int total, int successful, int skipped) {
        this(total, successful, skipped, true);
    }

    /**
     * If finalState = true, creates a Clusters object in its "final" state, where successful + skipped = total
     * If finalState = false, then the above condition is not enforced. This would happen in a case
     * where an async search is happening and the Clusters state will be incrementally updated
     * as cluster searches complete or fail.
     */
    protected Clusters(int total, int successful, int skipped, boolean finalState) {
        assert total >= 0 && successful >= 0 && skipped >= 0 : "total: " + total + " successful: " + successful + " skipped: " + skipped;
        assert finalState == false || (successful <= total && skipped == total - successful)
            : "total: " + total + " successful: " + successful + " skipped: " + skipped;
        this.total = total;
        this.successful = successful;
        this.skipped = skipped;
        this.remoteClusters = -1;  // means "unknown" and not needed for this usage
        this.ccsMinimizeRoundtrips = false;
    }

    Clusters(StreamInput in) throws IOException {
        this(in.readVInt(), in.readVInt(), in.readVInt());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(total);
        out.writeVInt(successful);
        out.writeVInt(skipped);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (total > 0) {
            builder.startObject(_CLUSTERS_FIELD.getPreferredName());
            builder.field(TOTAL_FIELD.getPreferredName(), total);
            builder.field(SUCCESSFUL_FIELD.getPreferredName(), successful);
            builder.field(SKIPPED_FIELD.getPreferredName(), skipped);
            builder.endObject();
        }
        return builder;
    }

    /**
     * @return how many total clusters the search was requested to be executed on
     */
    public int getTotal() {
        return total;
    }

    /**
     * @return how many total clusters the search was executed successfully on
     */
    public int getSuccessful() {
        return successful;
    }

    /**
     * @return how many total clusters were used during the execution of the search request
     */
    public int getSkipped() {
        return skipped;
    }

    /**
     * @return how many remote clusters were using during the execution of the search request
     * If not set, returns -1, meaning 'unknown'.
     */
    public int getRemoteClusters() {
        return remoteClusters;
    }

    /**
     * @return whether this search was a cross cluster search done with ccsMinimizeRoundtrips=true
     */
    public boolean isCcsMinimizeRoundtrips() {
        return ccsMinimizeRoundtrips;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Clusters clusters = (Clusters) o;
        return total == clusters.total && successful == clusters.successful && skipped == clusters.skipped;
    }

    @Override
    public int hashCode() {
        return Objects.hash(total, successful, skipped);
    }

    @Override
    public String toString() {
        return "Clusters{total=" + total + ", successful=" + successful + ", skipped=" + skipped + '}';
    }

    public String toStringExtended() {
        return Strings.format(
            "Clusters{total=%d, successful=%d, skipped=%d, remote=%d, ccsMinimizeRoundtrips=%s}",
            total,
            successful,
            skipped,
            remoteClusters,
            ccsMinimizeRoundtrips
        );
    }
}
