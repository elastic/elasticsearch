/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.rank.context.QueryPhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.QueryPhaseRankShardContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankShardContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RankDocsRankBuilder extends RankBuilder {

    public static final String NAME = "rankdocs_rank_builder";

    private final List<String> retrieverNames;

    public RankDocsRankBuilder(StreamInput in) throws IOException {
        super(in);
        retrieverNames = in.readCollectionAsList(StreamInput::readOptionalString);
    }

    public RankDocsRankBuilder(int rankWindowSize, List<String> retrieverNames) {
        super(rankWindowSize);
        this.retrieverNames = Collections.unmodifiableList(retrieverNames);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeCollection(retrieverNames, StreamOutput::writeOptionalString);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("retriever_names", retrieverNames);
    }

    @Override
    public boolean isCompoundBuilder() {
        return false;
    }

    @Override
    public Explanation explainHit(Explanation baseExplanation, RankDoc scoreDoc, List<String> queryNames) {
        // we know that when arriving from a CompoundRetriever, the base explanation will contain both the actual
        // RankDocsQuery explanation, as well info on the disjunction of the sub queries,
        // which however is of no actual interest on its own. So in this method we repackage the explanation so that we would have
        // the ranker's overall score as info, keep the `RankDocsQuery#explain` as the main explanation, and add the sub queries'
        // explanations
        // computed from the disjunction.
        if (false == baseExplanation.isMatch() || baseExplanation.getDetails().length < 2) {
            return baseExplanation;
        }
        List<Explanation> explanations = new ArrayList<>();
        int subQueriesAdded = 0;
        for (int i = 0; i < baseExplanation.getDetails()[0].getDetails().length; i++) {
            Explanation nestedExplanation = baseExplanation.getDetails()[0].getDetails()[i];
            final String queryAlias = retrieverNames.get(i) == null ? "" : " [" + retrieverNames.get(i) + "]";
            if (nestedExplanation.isMatch()) {
                explanations.add(
                    Explanation.match(
                        nestedExplanation.getValue(),
                        nestedExplanation.getDescription() + queryAlias,
                        baseExplanation.getDetails()[++subQueriesAdded]
                    )
                );
            } else {
                explanations.add(Explanation.noMatch(baseExplanation.getDetails()[0].getDetails()[i].getDescription() + queryAlias));
            }
        }
        return Explanation.match(
            baseExplanation.getDetails()[0].getValue(),
            baseExplanation.getDetails()[0].getDescription(),
            explanations.toArray(new Explanation[0])
        );
    }

    @Override
    public QueryPhaseRankShardContext buildQueryPhaseShardContext(List<Query> queries, int from) {
        return null;
    }

    @Override
    public QueryPhaseRankCoordinatorContext buildQueryPhaseCoordinatorContext(int size, int from) {
        return null;
    }

    @Override
    public RankFeaturePhaseRankShardContext buildRankFeaturePhaseShardContext() {
        return null;
    }

    @Override
    public RankFeaturePhaseRankCoordinatorContext buildRankFeaturePhaseCoordinatorContext(int size, int from, Client client) {
        return null;
    }

    @Override
    protected boolean doEquals(RankBuilder other) {
        return true;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.RRF_QUERY_REWRITE;
    }
}
