/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

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
     * Returns how many total clusters the search was requested to be executed on
     */
    public int getTotal() {
        return total;
    }

    /**
     * Returns how many total clusters the search was executed successfully on
     */
    public int getSuccessful() {
        return successful;
    }

    /**
     * Returns how many total clusters were during the execution of the search request
     */
    public int getSkipped() {
        return skipped;
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
}
