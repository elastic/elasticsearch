/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rank.context.QueryPhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.QueryPhaseRankShardContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankShardContext;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * {@code RankBuilder} is used as a base class to manage input, parsing, and subsequent generation of appropriate contexts
 * for handling searches that require multiple queries and/or ranking steps for global rank relevance.
 */
public abstract class RankBuilder implements VersionedNamedWriteable, ToXContentObject {

    public static final ParseField RANK_WINDOW_SIZE_FIELD = new ParseField("rank_window_size");

    public static final int DEFAULT_RANK_WINDOW_SIZE = SearchService.DEFAULT_SIZE;

    private final int rankWindowSize;

    public RankBuilder(int rankWindowSize) {
        this.rankWindowSize = rankWindowSize;
    }

    public RankBuilder(StreamInput in) throws IOException {
        rankWindowSize = in.readVInt();
    }

    public final void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(rankWindowSize);
        doWriteTo(out);
    }

    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(getWriteableName());
        builder.field(RANK_WINDOW_SIZE_FIELD.getPreferredName(), rankWindowSize);
        doXContent(builder, params);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    protected abstract void doXContent(XContentBuilder builder, Params params) throws IOException;

    public int rankWindowSize() {
        return rankWindowSize;
    }

    /**
     * Specify whether this rank builder is a compound builder or not. A compound builder is a rank builder that requires
     * two or more queries to be executed in order to generate the final result.
     */
    public abstract boolean isCompoundBuilder();

    /**
     * Generates an {@code Explanation} on how the final score for the provided {@code RankDoc} is computed for the given `RankBuilder`.
     * In addition to the base explanation to enrich, we also have access to the query names that were provided in the request,
     * so that we can have direct association with the user provided query.
     */
    public abstract Explanation explainHit(Explanation baseExplanation, RankDoc scoreDoc, List<String> queryNames);

    /**
     * Generates a context used to execute required searches during the query phase on the shard.
     */
    public abstract QueryPhaseRankShardContext buildQueryPhaseShardContext(List<Query> queries, int from);

    /**
     * Generates a context used to be executed on the coordinating node, that would combine all individual shard results.
     */
    public abstract QueryPhaseRankCoordinatorContext buildQueryPhaseCoordinatorContext(int size, int from);

    /**
     * Generates a context used to execute the rank feature phase on the shard. This is responsible for retrieving any needed
     * feature data, and passing them back to the coordinator through the appropriate {@link  RankShardResult}.
     */
    public abstract RankFeaturePhaseRankShardContext buildRankFeaturePhaseShardContext();

    /**
     * Generates a context used to perform global ranking during the RankFeature phase,
     * on the coordinator based on all the individual shard results. The output of this will be a `size` ranked list of ordered results,
     * which will then be passed to fetch phase.
     */
    public abstract RankFeaturePhaseRankCoordinatorContext buildRankFeaturePhaseCoordinatorContext(int size, int from, Client client);

    /**
     * Transforms the specific rank builder (as parsed through SearchSourceBuilder) to the corresponding retriever.
     * This is used to ensure smooth deprecation of `rank` and `sub_searches` and move towards the retriever framework
     */
    @UpdateForV10(owner = UpdateForV10.Owner.SEARCH_RELEVANCE) // remove for 10.0 once we remove support for the rank parameter in SearchAPI
    @Nullable
    public RetrieverBuilder toRetriever(SearchSourceBuilder searchSourceBuilder, Predicate<NodeFeature> clusterSupportsFeature) {
        return null;
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RankBuilder other = (RankBuilder) obj;
        return rankWindowSize == other.rankWindowSize && doEquals(other);
    }

    protected abstract boolean doEquals(RankBuilder other);

    @Override
    public final int hashCode() {
        return Objects.hash(getClass(), rankWindowSize, doHashCode());
    }

    protected abstract int doHashCode();

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
