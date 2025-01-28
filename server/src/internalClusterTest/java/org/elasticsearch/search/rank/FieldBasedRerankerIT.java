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
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.context.QueryPhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.QueryPhaseRankShardContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankShardContext;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.feature.RankFeatureShardResult;
import org.elasticsearch.search.rank.rerank.AbstractRerankerIT;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class FieldBasedRerankerIT extends AbstractRerankerIT {

    @Override
    protected RankBuilder getRankBuilder(int rankWindowSize, String rankFeatureField) {
        return new FieldBasedRankBuilder(rankWindowSize, rankFeatureField);
    }

    @Override
    protected RankBuilder getThrowingRankBuilder(
        int rankWindowSize,
        String rankFeatureField,
        AbstractRerankerIT.ThrowingRankBuilderType type
    ) {
        return new ThrowingRankBuilder(rankWindowSize, rankFeatureField, type.name());
    }

    @Override
    protected Collection<Class<? extends Plugin>> pluginsNeeded() {
        return Collections.singletonList(FieldBasedRerankerPlugin.class);
    }

    public static class FieldBasedRankBuilder extends RankBuilder {

        public static final ParseField FIELD_FIELD = new ParseField("field");
        static final ConstructingObjectParser<FieldBasedRankBuilder, Void> PARSER = new ConstructingObjectParser<>(
            "field_based_rank",
            args -> {
                int rankWindowSize = args[0] == null ? DEFAULT_RANK_WINDOW_SIZE : (int) args[0];
                String field = (String) args[1];
                if (field == null || field.isEmpty()) {
                    throw new IllegalArgumentException("Field cannot be null or empty");
                }
                return new FieldBasedRankBuilder(rankWindowSize, field);
            }
        );

        static {
            PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
            PARSER.declareString(constructorArg(), FIELD_FIELD);
        }

        protected final String field;

        public static FieldBasedRankBuilder fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        public FieldBasedRankBuilder(final int rankWindowSize, final String field) {
            super(rankWindowSize);
            this.field = field;
        }

        public FieldBasedRankBuilder(StreamInput in) throws IOException {
            super(in);
            this.field = in.readString();
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            out.writeString(field);
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(FIELD_FIELD.getPreferredName(), field);
        }

        @Override
        public boolean isCompoundBuilder() {
            return false;
        }

        @Override
        public Explanation explainHit(Explanation baseExplanation, RankDoc scoreDoc, List<String> queryNames) {
            return baseExplanation;
        }

        @Override
        public QueryPhaseRankShardContext buildQueryPhaseShardContext(List<Query> queries, int from) {
            return new QueryPhaseRankShardContext(queries, rankWindowSize()) {
                @Override
                public RankShardResult combineQueryPhaseResults(List<TopDocs> rankResults) {
                    Map<Integer, RankFeatureDoc> rankDocs = new HashMap<>();
                    rankResults.forEach(topDocs -> {
                        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                            rankDocs.compute(scoreDoc.doc, (key, value) -> {
                                if (value == null) {
                                    return new RankFeatureDoc(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex);
                                } else {
                                    value.score = Math.max(scoreDoc.score, rankDocs.get(scoreDoc.doc).score);
                                    return value;
                                }
                            });
                        }
                    });
                    RankFeatureDoc[] sortedResults = rankDocs.values().toArray(RankFeatureDoc[]::new);
                    Arrays.sort(sortedResults, (o1, o2) -> Float.compare(o2.score, o1.score));
                    return new RankFeatureShardResult(sortedResults);
                }
            };
        }

        @Override
        public QueryPhaseRankCoordinatorContext buildQueryPhaseCoordinatorContext(int size, int from) {
            return new QueryPhaseRankCoordinatorContext(rankWindowSize()) {
                @Override
                public ScoreDoc[] rankQueryPhaseResults(
                    List<QuerySearchResult> querySearchResults,
                    SearchPhaseController.TopDocsStats topDocStats
                ) {
                    List<RankFeatureDoc> rankDocs = new ArrayList<>();
                    for (int i = 0; i < querySearchResults.size(); i++) {
                        QuerySearchResult querySearchResult = querySearchResults.get(i);
                        RankFeatureShardResult shardResult = (RankFeatureShardResult) querySearchResult.getRankShardResult();
                        for (RankFeatureDoc frd : shardResult.rankFeatureDocs) {
                            frd.shardIndex = i;
                            rankDocs.add(frd);
                        }
                    }
                    // no support for sort field atm
                    // should pass needed info to make use of org.elasticsearch.action.search.SearchPhaseController.sortDocs?
                    rankDocs.sort(Comparator.comparing((RankFeatureDoc doc) -> doc.score).reversed());
                    RankFeatureDoc[] topResults = rankDocs.stream().limit(rankWindowSize).toArray(RankFeatureDoc[]::new);

                    assert topDocStats.fetchHits == 0;
                    topDocStats.fetchHits = topResults.length;

                    return topResults;
                }
            };
        }

        @Override
        public RankFeaturePhaseRankShardContext buildRankFeaturePhaseShardContext() {
            return new RankFeaturePhaseRankShardContext(field) {
                @Override
                public RankShardResult buildRankFeatureShardResult(SearchHits hits, int shardId) {
                    try {
                        RankFeatureDoc[] rankFeatureDocs = new RankFeatureDoc[hits.getHits().length];
                        for (int i = 0; i < hits.getHits().length; i++) {
                            rankFeatureDocs[i] = new RankFeatureDoc(hits.getHits()[i].docId(), hits.getHits()[i].getScore(), shardId);
                            rankFeatureDocs[i].featureData(hits.getHits()[i].field(field).getValue().toString());
                        }
                        return new RankFeatureShardResult(rankFeatureDocs);
                    } catch (Exception ex) {
                        throw ex;
                    }
                }
            };
        }

        @Override
        public RankFeaturePhaseRankCoordinatorContext buildRankFeaturePhaseCoordinatorContext(int size, int from, Client client) {
            return new RankFeaturePhaseRankCoordinatorContext(size, from, rankWindowSize()) {
                @Override
                protected void computeScores(RankFeatureDoc[] featureDocs, ActionListener<float[]> scoreListener) {
                    float[] scores = new float[featureDocs.length];
                    for (int i = 0; i < featureDocs.length; i++) {
                        scores[i] = Float.parseFloat(featureDocs[i].featureData);
                    }
                    scoreListener.onResponse(scores);
                }
            };
        }

        @Override
        protected boolean doEquals(RankBuilder other) {
            return other instanceof FieldBasedRankBuilder && Objects.equals(field, ((FieldBasedRankBuilder) other).field);
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(field);
        }

        @Override
        public String getWriteableName() {
            return "field_based_rank";
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_8_15_0;
        }
    }

    public static class ThrowingRankBuilder extends FieldBasedRankBuilder {

        public enum ThrowingRankBuilderType {
            THROWING_QUERY_PHASE_SHARD_CONTEXT,
            THROWING_QUERY_PHASE_COORDINATOR_CONTEXT,
            THROWING_RANK_FEATURE_PHASE_SHARD_CONTEXT,
            THROWING_RANK_FEATURE_PHASE_COORDINATOR_CONTEXT;
        }

        protected final ThrowingRankBuilderType throwingRankBuilderType;

        public static final ParseField FIELD_FIELD = new ParseField("field");
        public static final ParseField THROWING_TYPE_FIELD = new ParseField("throwing-type");
        static final ConstructingObjectParser<ThrowingRankBuilder, Void> PARSER = new ConstructingObjectParser<>("throwing_rank", args -> {
            int rankWindowSize = args[0] == null ? DEFAULT_RANK_WINDOW_SIZE : (int) args[0];
            String field = (String) args[1];
            if (field == null || field.isEmpty()) {
                throw new IllegalArgumentException("Field cannot be null or empty");
            }
            String throwingType = (String) args[2];
            return new ThrowingRankBuilder(rankWindowSize, field, throwingType);
        });

        static {
            PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
            PARSER.declareString(constructorArg(), FIELD_FIELD);
            PARSER.declareString(constructorArg(), THROWING_TYPE_FIELD);
        }

        public static FieldBasedRankBuilder fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        public ThrowingRankBuilder(final int rankWindowSize, final String field, final String throwingType) {
            super(rankWindowSize, field);
            this.throwingRankBuilderType = ThrowingRankBuilderType.valueOf(throwingType);
        }

        public ThrowingRankBuilder(StreamInput in) throws IOException {
            super(in);
            this.throwingRankBuilderType = in.readEnum(ThrowingRankBuilderType.class);
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            super.doWriteTo(out);
            out.writeEnum(throwingRankBuilderType);
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            super.doXContent(builder, params);
            builder.field(THROWING_TYPE_FIELD.getPreferredName(), throwingRankBuilderType);
        }

        @Override
        public String getWriteableName() {
            return "throwing_rank";
        }

        @Override
        public QueryPhaseRankShardContext buildQueryPhaseShardContext(List<Query> queries, int from) {
            if (this.throwingRankBuilderType == ThrowingRankBuilderType.THROWING_QUERY_PHASE_SHARD_CONTEXT)
                return new QueryPhaseRankShardContext(queries, rankWindowSize()) {
                    @Override
                    public RankShardResult combineQueryPhaseResults(List<TopDocs> rankResults) {
                        throw new UnsupportedOperationException("qps - simulated failure");
                    }
                };
            else {
                return super.buildQueryPhaseShardContext(queries, from);
            }
        }

        @Override
        public QueryPhaseRankCoordinatorContext buildQueryPhaseCoordinatorContext(int size, int from) {
            if (this.throwingRankBuilderType == ThrowingRankBuilderType.THROWING_QUERY_PHASE_COORDINATOR_CONTEXT)
                return new QueryPhaseRankCoordinatorContext(rankWindowSize()) {
                    @Override
                    public ScoreDoc[] rankQueryPhaseResults(
                        List<QuerySearchResult> querySearchResults,
                        SearchPhaseController.TopDocsStats topDocStats
                    ) {
                        throw new UnsupportedOperationException("qpc - simulated failure");
                    }
                };
            else {
                return super.buildQueryPhaseCoordinatorContext(size, from);
            }
        }

        @Override
        public RankFeaturePhaseRankShardContext buildRankFeaturePhaseShardContext() {
            if (this.throwingRankBuilderType == ThrowingRankBuilderType.THROWING_RANK_FEATURE_PHASE_SHARD_CONTEXT)
                return new RankFeaturePhaseRankShardContext(field) {
                    @Override
                    public RankShardResult buildRankFeatureShardResult(SearchHits hits, int shardId) {
                        throw new UnsupportedOperationException("rfs - simulated failure");
                    }
                };
            else {
                return super.buildRankFeaturePhaseShardContext();
            }
        }

        @Override
        public RankFeaturePhaseRankCoordinatorContext buildRankFeaturePhaseCoordinatorContext(int size, int from, Client client) {
            if (this.throwingRankBuilderType == ThrowingRankBuilderType.THROWING_RANK_FEATURE_PHASE_COORDINATOR_CONTEXT)
                return new RankFeaturePhaseRankCoordinatorContext(size, from, rankWindowSize()) {
                    @Override
                    protected void computeScores(RankFeatureDoc[] featureDocs, ActionListener<float[]> scoreListener) {
                        throw new UnsupportedOperationException("rfc - simulated failure");
                    }
                };
            else {
                return super.buildRankFeaturePhaseCoordinatorContext(size, from, client);
            }
        }
    }

    public static class FieldBasedRerankerPlugin extends Plugin implements SearchPlugin {

        private static final String FIELD_BASED_RANK_BUILDER_NAME = "field_based_rank";
        private static final String THROWING_RANK_BUILDER_NAME = "throwing_rank";

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return List.of(
                new NamedWriteableRegistry.Entry(RankBuilder.class, FIELD_BASED_RANK_BUILDER_NAME, FieldBasedRankBuilder::new),
                new NamedWriteableRegistry.Entry(RankBuilder.class, THROWING_RANK_BUILDER_NAME, ThrowingRankBuilder::new)
            );
        }

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return List.of(
                new NamedXContentRegistry.Entry(
                    RankBuilder.class,
                    new ParseField(FIELD_BASED_RANK_BUILDER_NAME),
                    FieldBasedRankBuilder::fromXContent
                ),
                new NamedXContentRegistry.Entry(
                    RankBuilder.class,
                    new ParseField(THROWING_RANK_BUILDER_NAME),
                    ThrowingRankBuilder::fromXContent
                )
            );
        }
    }
}
