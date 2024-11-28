/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.retriever;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class RuleQueryRankDoc extends RankDoc {

    public static final String NAME = "query_rule_rank_doc";

    public final List<String> rulesetIds;
    public final Map<String, Object> matchCriteria;

    public RuleQueryRankDoc(int doc, float score, int shardIndex, List<String> rulesetIds, Map<String, Object> matchCriteria) {
        super(doc, score, shardIndex);
        this.rulesetIds = rulesetIds;
        this.matchCriteria = matchCriteria;
    }

    public RuleQueryRankDoc(StreamInput in) throws IOException {
        super(in);
        rulesetIds = in.readStringCollectionAsImmutableList();
        matchCriteria = in.readGenericMap();
    }

    @Override
    public Explanation explain(Explanation[] sources, String[] queryNames) {

        return Explanation.match(
            score,
            "query rules evaluated rules from rulesets " + rulesetIds + " and match criteria " + matchCriteria,
            sources
        );
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeStringCollection(rulesetIds);
        out.writeGenericMap(matchCriteria);
    }

    @Override
    public boolean doEquals(RankDoc rd) {
        RuleQueryRankDoc rqrd = (RuleQueryRankDoc) rd;
        return Objects.equals(rulesetIds, rqrd.rulesetIds) && Objects.equals(matchCriteria, rqrd.matchCriteria);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(rulesetIds, matchCriteria);
    }

    @Override
    public String toString() {
        return "QueryRuleRankDoc{"
            + "doc="
            + doc
            + ", shardIndex="
            + shardIndex
            + ", score="
            + score
            + ", rulesetIds="
            + rulesetIds
            + ", matchCriteria="
            + matchCriteria
            + "}";
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.array("rulesetIds", rulesetIds.toArray());
        builder.startObject("matchCriteria");
        builder.mapContents(matchCriteria);
        builder.endObject();
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.QUERY_RULES_RETRIEVER;
    }
}
