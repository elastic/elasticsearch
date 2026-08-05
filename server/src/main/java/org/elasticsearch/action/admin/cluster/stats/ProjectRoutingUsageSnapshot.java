/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A point-in-time snapshot of {@link ProjectRoutingUsageHolder} counters for one node.
 *
 * <p>Instances are created on each node, shipped to the coordinator as part of
 * {@link ClusterStatsNodeResponse}, and accumulated additively with {@link #add(ProjectRoutingUsageSnapshot)}.
 * The accumulated snapshot is then rendered into the {@code project_routing} top-level block of
 * {@code GET _cluster/stats}.
 */
public class ProjectRoutingUsageSnapshot implements Writeable, ToXContentFragment {

    private long searchQueriesTotal;
    private long searchWithProjectRouting;
    private long searchWithAliasOrigin;
    private long searchWithAliasWildcard;
    private long searchWithCustomTags;
    private long searchWithNamedExpression;
    private long searchFailures;

    private long esqlQueriesTotal;
    private long esqlWithProjectRouting;
    private long esqlWithAliasOrigin;
    private long esqlWithAliasWildcard;
    private long esqlWithCustomTags;
    private long esqlWithNamedExpression;
    private long esqlWithSet;
    private long esqlFailures;

    /** Creates an empty snapshot suitable for accumulating node snapshots into. */
    public ProjectRoutingUsageSnapshot() {}

    /** Creates a snapshot from the provided counter values (called by {@link ProjectRoutingUsageHolder#getSnapshot()}). */
    public ProjectRoutingUsageSnapshot(
        long searchQueriesTotal,
        long searchWithProjectRouting,
        long searchWithAliasOrigin,
        long searchWithAliasWildcard,
        long searchWithCustomTags,
        long searchWithNamedExpression,
        long searchFailures,
        long esqlQueriesTotal,
        long esqlWithProjectRouting,
        long esqlWithAliasOrigin,
        long esqlWithAliasWildcard,
        long esqlWithCustomTags,
        long esqlWithNamedExpression,
        long esqlWithSet,
        long esqlFailures
    ) {
        this.searchQueriesTotal = searchQueriesTotal;
        this.searchWithProjectRouting = searchWithProjectRouting;
        this.searchWithAliasOrigin = searchWithAliasOrigin;
        this.searchWithAliasWildcard = searchWithAliasWildcard;
        this.searchWithCustomTags = searchWithCustomTags;
        this.searchWithNamedExpression = searchWithNamedExpression;
        this.searchFailures = searchFailures;
        this.esqlQueriesTotal = esqlQueriesTotal;
        this.esqlWithProjectRouting = esqlWithProjectRouting;
        this.esqlWithAliasOrigin = esqlWithAliasOrigin;
        this.esqlWithAliasWildcard = esqlWithAliasWildcard;
        this.esqlWithCustomTags = esqlWithCustomTags;
        this.esqlWithNamedExpression = esqlWithNamedExpression;
        this.esqlWithSet = esqlWithSet;
        this.esqlFailures = esqlFailures;
    }

    public ProjectRoutingUsageSnapshot(StreamInput in) throws IOException {
        searchQueriesTotal = in.readVLong();
        searchWithProjectRouting = in.readVLong();
        searchWithAliasOrigin = in.readVLong();
        searchWithAliasWildcard = in.readVLong();
        searchWithCustomTags = in.readVLong();
        searchWithNamedExpression = in.readVLong();
        searchFailures = in.readVLong();
        esqlQueriesTotal = in.readVLong();
        esqlWithProjectRouting = in.readVLong();
        esqlWithAliasOrigin = in.readVLong();
        esqlWithAliasWildcard = in.readVLong();
        esqlWithCustomTags = in.readVLong();
        esqlWithNamedExpression = in.readVLong();
        esqlWithSet = in.readVLong();
        esqlFailures = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(searchQueriesTotal);
        out.writeVLong(searchWithProjectRouting);
        out.writeVLong(searchWithAliasOrigin);
        out.writeVLong(searchWithAliasWildcard);
        out.writeVLong(searchWithCustomTags);
        out.writeVLong(searchWithNamedExpression);
        out.writeVLong(searchFailures);
        out.writeVLong(esqlQueriesTotal);
        out.writeVLong(esqlWithProjectRouting);
        out.writeVLong(esqlWithAliasOrigin);
        out.writeVLong(esqlWithAliasWildcard);
        out.writeVLong(esqlWithCustomTags);
        out.writeVLong(esqlWithNamedExpression);
        out.writeVLong(esqlWithSet);
        out.writeVLong(esqlFailures);
    }

    /**
     * Additively merges {@code other} into this snapshot. Called on the coordinator to combine node responses.
     */
    public void add(ProjectRoutingUsageSnapshot other) {
        if (other == null) return;
        searchQueriesTotal += other.searchQueriesTotal;
        searchWithProjectRouting += other.searchWithProjectRouting;
        searchWithAliasOrigin += other.searchWithAliasOrigin;
        searchWithAliasWildcard += other.searchWithAliasWildcard;
        searchWithCustomTags += other.searchWithCustomTags;
        searchWithNamedExpression += other.searchWithNamedExpression;
        searchFailures += other.searchFailures;
        esqlQueriesTotal += other.esqlQueriesTotal;
        esqlWithProjectRouting += other.esqlWithProjectRouting;
        esqlWithAliasOrigin += other.esqlWithAliasOrigin;
        esqlWithAliasWildcard += other.esqlWithAliasWildcard;
        esqlWithCustomTags += other.esqlWithCustomTags;
        esqlWithNamedExpression += other.esqlWithNamedExpression;
        esqlWithSet += other.esqlWithSet;
        esqlFailures += other.esqlFailures;
    }

    public long getSearchQueriesTotal() {
        return searchQueriesTotal;
    }

    public long getSearchWithProjectRouting() {
        return searchWithProjectRouting;
    }

    public long getSearchWithAliasOrigin() {
        return searchWithAliasOrigin;
    }

    public long getSearchWithAliasWildcard() {
        return searchWithAliasWildcard;
    }

    public long getSearchWithCustomTags() {
        return searchWithCustomTags;
    }

    public long getSearchWithNamedExpression() {
        return searchWithNamedExpression;
    }

    public long getSearchFailures() {
        return searchFailures;
    }

    public long getEsqlQueriesTotal() {
        return esqlQueriesTotal;
    }

    public long getEsqlWithProjectRouting() {
        return esqlWithProjectRouting;
    }

    public long getEsqlWithAliasOrigin() {
        return esqlWithAliasOrigin;
    }

    public long getEsqlWithAliasWildcard() {
        return esqlWithAliasWildcard;
    }

    public long getEsqlWithCustomTags() {
        return esqlWithCustomTags;
    }

    public long getEsqlWithNamedExpression() {
        return esqlWithNamedExpression;
    }

    public long getEsqlWithSet() {
        return esqlWithSet;
    }

    public long getEsqlFailures() {
        return esqlFailures;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProjectRoutingUsageSnapshot other = (ProjectRoutingUsageSnapshot) o;
        return searchQueriesTotal == other.searchQueriesTotal
            && searchWithProjectRouting == other.searchWithProjectRouting
            && searchWithAliasOrigin == other.searchWithAliasOrigin
            && searchWithAliasWildcard == other.searchWithAliasWildcard
            && searchWithCustomTags == other.searchWithCustomTags
            && searchWithNamedExpression == other.searchWithNamedExpression
            && searchFailures == other.searchFailures
            && esqlQueriesTotal == other.esqlQueriesTotal
            && esqlWithProjectRouting == other.esqlWithProjectRouting
            && esqlWithAliasOrigin == other.esqlWithAliasOrigin
            && esqlWithAliasWildcard == other.esqlWithAliasWildcard
            && esqlWithCustomTags == other.esqlWithCustomTags
            && esqlWithNamedExpression == other.esqlWithNamedExpression
            && esqlWithSet == other.esqlWithSet
            && esqlFailures == other.esqlFailures;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            searchQueriesTotal,
            searchWithProjectRouting,
            searchWithAliasOrigin,
            searchWithAliasWildcard,
            searchWithCustomTags,
            searchWithNamedExpression,
            searchFailures,
            esqlQueriesTotal,
            esqlWithProjectRouting,
            esqlWithAliasOrigin,
            esqlWithAliasWildcard,
            esqlWithCustomTags,
            esqlWithNamedExpression,
            esqlWithSet,
            esqlFailures
        );
    }

    /**
     * Emits the {@code search} and {@code esql} sub-objects inside the {@code project_routing} block.
     * The caller is responsible for opening and closing the {@code project_routing} object and for emitting
     * the top-level {@code queries} sum. Subsections with zero counts are suppressed.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (searchQueriesTotal > 0) {
            builder.startObject("search");
            builder.field("queries", searchQueriesTotal);
            builder.field("queries_project_routing", searchWithProjectRouting);
            builder.field("alias_origin", searchWithAliasOrigin);
            builder.field("alias_wildcard", searchWithAliasWildcard);
            builder.field("custom_tags", searchWithCustomTags);
            builder.field("named_expression", searchWithNamedExpression);
            builder.field("failures", searchFailures);
            builder.endObject();
        }
        if (esqlQueriesTotal > 0) {
            builder.startObject("esql");
            builder.field("queries", esqlQueriesTotal);
            builder.field("queries_project_routing", esqlWithProjectRouting);
            builder.field("alias_origin", esqlWithAliasOrigin);
            builder.field("alias_wildcard", esqlWithAliasWildcard);
            builder.field("custom_tags", esqlWithCustomTags);
            builder.field("named_expression", esqlWithNamedExpression);
            builder.field("in_SET", esqlWithSet);
            builder.field("failures", esqlFailures);
            builder.endObject();
        }
        return builder;
    }
}
