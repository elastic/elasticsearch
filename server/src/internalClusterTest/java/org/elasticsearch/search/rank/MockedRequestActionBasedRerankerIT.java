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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.context.QueryPhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.QueryPhaseRankShardContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankShardContext;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.rerank.AbstractRerankerIT;
import org.elasticsearch.search.rank.rerank.RerankingQueryPhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.rerank.RerankingQueryPhaseRankShardContext;
import org.elasticsearch.search.rank.rerank.RerankingRankFeaturePhaseRankShardContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class MockedRequestActionBasedRerankerIT extends AbstractRerankerIT {

    private static final TestRerankingActionType TEST_RERANKING_ACTION_TYPE = new TestRerankingActionType("internal:test_reranking_action");

    private static final String inferenceId = "inference-id";
    private static final String inferenceText = "inference-text";
    private static final float minScore = 0.0f;

    @Override
    protected RankBuilder getRankBuilder(int rankWindowSize, String rankFeatureField) {
        return new MockRequestActionBasedRankBuilder(rankWindowSize, rankFeatureField, inferenceId, inferenceText, minScore);
    }

    @Override
    protected RankBuilder getThrowingRankBuilder(int rankWindowSize, String rankFeatureField, ThrowingRankBuilderType type) {
        return new ThrowingMockRequestActionBasedRankBuilder(
            rankWindowSize,
            rankFeatureField,
            inferenceId,
            inferenceText,
            minScore,
            type.name()
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> pluginsNeeded() {
        return List.of(RerankerServicePlugin.class, RequestActionBasedRerankerPlugin.class);
    }

    public static class RerankerServicePlugin extends Plugin implements ActionPlugin {

        @Override
        public Collection<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(new ActionHandler<>(TEST_RERANKING_ACTION_TYPE, TestRerankingTransportAction.class));
        }
    }

    public static class RequestActionBasedRerankerPlugin extends Plugin implements SearchPlugin {

        private static final String REQUEST_ACTION_BASED_RANK_BUILDER_NAME = "request_action_based_rank";
        private static final String THROWING_REQUEST_ACTION_BASED_RANK_BUILDER_NAME = "throwing_request_action_based_rank";

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return List.of(
                new NamedWriteableRegistry.Entry(
                    RankBuilder.class,
                    REQUEST_ACTION_BASED_RANK_BUILDER_NAME,
                    MockRequestActionBasedRankBuilder::new
                ),
                new NamedWriteableRegistry.Entry(
                    RankBuilder.class,
                    THROWING_REQUEST_ACTION_BASED_RANK_BUILDER_NAME,
                    ThrowingMockRequestActionBasedRankBuilder::new
                )
            );
        }

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return List.of(
                new NamedXContentRegistry.Entry(
                    RankBuilder.class,
                    new ParseField(REQUEST_ACTION_BASED_RANK_BUILDER_NAME),
                    MockRequestActionBasedRankBuilder::fromXContent
                ),
                new NamedXContentRegistry.Entry(
                    RankBuilder.class,
                    new ParseField(THROWING_REQUEST_ACTION_BASED_RANK_BUILDER_NAME),
                    ThrowingMockRequestActionBasedRankBuilder::fromXContent
                )
            );
        }
    }

    public static class TestRerankingActionType extends ActionType<TestRerankingActionResponse> {
        TestRerankingActionType(String name) {
            super(name);
        }
    }

    public static class TestRerankingActionRequest extends ActionRequest {

        private final List<String> docFeatures;

        public TestRerankingActionRequest(List<String> docFeatures) {
            super();
            this.docFeatures = docFeatures;
        }

        public TestRerankingActionRequest(StreamInput in) throws IOException {
            super(in);
            this.docFeatures = in.readCollectionAsList(StreamInput::readString);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeCollection(docFeatures, StreamOutput::writeString);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public boolean shouldFail() {
            return false;
        }
    }

    public static class TestThrowingRerankingActionRequest extends TestRerankingActionRequest {

        public TestThrowingRerankingActionRequest(List<String> docFeatures) {
            super(docFeatures);
        }

        public TestThrowingRerankingActionRequest(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public boolean shouldFail() {
            return true;
        }
    }

    public static class TestRerankingActionResponse extends ActionResponse {

        private final List<Float> scores;

        public TestRerankingActionResponse(List<Float> scores) {
            super();
            this.scores = scores;
        }

        public TestRerankingActionResponse(StreamInput in) throws IOException {
            super(in);
            this.scores = in.readCollectionAsList(StreamInput::readFloat);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(scores, StreamOutput::writeFloat);
        }
    }

    public static class TestRerankingTransportAction extends HandledTransportAction<
        TestRerankingActionRequest,
        TestRerankingActionResponse> {
        @Inject
        public TestRerankingTransportAction(TransportService transportService, ActionFilters actionFilters) {
            super(
                TEST_RERANKING_ACTION_TYPE.name(),
                transportService,
                actionFilters,
                TestRerankingActionRequest::new,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
        }

        @Override
        protected void doExecute(Task task, TestRerankingActionRequest request, ActionListener<TestRerankingActionResponse> listener) {
            if (request.shouldFail()) {
                listener.onFailure(new UnsupportedOperationException("simulated failure"));
            } else {
                List<String> featureData = request.docFeatures;
                List<Float> scores = featureData.stream().map(Float::parseFloat).toList();
                listener.onResponse(new TestRerankingActionResponse(scores));
            }
        }
    }

    public static class TestRerankingRankFeaturePhaseRankCoordinatorContext extends RankFeaturePhaseRankCoordinatorContext {

        private final String inferenceId;
        private final String inferenceText;
        private final Client client;

        TestRerankingRankFeaturePhaseRankCoordinatorContext(
            int size,
            int from,
            int windowSize,
            Client client,
            String inferenceId,
            String inferenceText,
            float minScore
        ) {
            super(size, from, windowSize);
            this.client = client;
            this.inferenceId = inferenceId;
            this.inferenceText = inferenceText;
        }

        protected TestRerankingActionRequest generateRequest(List<String> docFeatures) {
            return new TestRerankingActionRequest(docFeatures);
        }

        protected ActionType<TestRerankingActionResponse> actionType() {
            return TEST_RERANKING_ACTION_TYPE;
        }

        protected float[] extractScoresFromResponse(TestRerankingActionResponse response) {
            float[] scores = new float[response.scores.size()];
            for (int i = 0; i < response.scores.size(); i++) {
                scores[i] = response.scores.get(i);
            }
            return scores;
        }

        protected void computeScores(RankFeatureDoc[] featureDocs, ActionListener<float[]> scoreListener) {
            // Wrap the provided rankListener to an ActionListener that would handle the response from the inference service
            // and then pass the results
            final ActionListener<TestRerankingActionResponse> actionListener = scoreListener.delegateFailureAndWrap((l, r) -> {
                float[] scores = extractScoresFromResponse(r);
                assert scores.length == featureDocs.length;
                l.onResponse(scores);
            });

            List<String> featureData = Arrays.stream(featureDocs).map(x -> x.featureData).toList();
            TestRerankingActionRequest request = generateRequest(featureData);
            try {
                ActionType<TestRerankingActionResponse> action = actionType();
                client.execute(action, request, actionListener);
            } finally {
                if (request != null) {
                    request.decRef();
                }
            }
        }
    }

    public static class MockRequestActionBasedRankBuilder extends RankBuilder {

        public static final ParseField FIELD_FIELD = new ParseField("field");
        public static final ParseField INFERENCE_ID = new ParseField("inference_id");
        public static final ParseField INFERENCE_TEXT = new ParseField("inference_text");
        public static final ParseField MIN_SCORE_FIELD = new ParseField("min_score");
        static final ConstructingObjectParser<MockRequestActionBasedRankBuilder, Void> PARSER = new ConstructingObjectParser<>(
            "request_action_based_rank",
            args -> {
                int rankWindowSize = args[0] == null ? DEFAULT_RANK_WINDOW_SIZE : (int) args[0];
                String field = (String) args[1];
                if (field == null || field.isEmpty()) {
                    throw new IllegalArgumentException("Field cannot be null or empty");
                }
                final String inferenceId = (String) args[2];
                final String inferenceText = (String) args[3];
                final float minScore = (float) args[4];
                return new MockRequestActionBasedRankBuilder(rankWindowSize, field, inferenceId, inferenceText, minScore);
            }
        );

        static {
            PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
            PARSER.declareString(constructorArg(), FIELD_FIELD);
            PARSER.declareString(constructorArg(), INFERENCE_ID);
            PARSER.declareString(constructorArg(), INFERENCE_TEXT);
        }

        protected final String field;
        protected final String inferenceId;
        protected final String inferenceText;
        protected final float minScore;

        public static MockRequestActionBasedRankBuilder fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        public MockRequestActionBasedRankBuilder(
            final int rankWindowSize,
            final String field,
            final String inferenceId,
            final String inferenceText,
            final float minScore
        ) {
            super(rankWindowSize);
            this.field = field;
            this.inferenceId = inferenceId;
            this.inferenceText = inferenceText;
            this.minScore = minScore;
        }

        public MockRequestActionBasedRankBuilder(StreamInput in) throws IOException {
            super(in);
            this.field = in.readString();
            this.inferenceId = in.readString();
            this.inferenceText = in.readString();
            this.minScore = in.readFloat();
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            out.writeString(field);
            out.writeString(inferenceId);
            out.writeString(inferenceText);
            out.writeFloat(minScore);
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(FIELD_FIELD.getPreferredName(), field);
            builder.field(INFERENCE_ID.getPreferredName(), inferenceId);
            builder.field(INFERENCE_TEXT.getPreferredName(), inferenceText);
            builder.field(MIN_SCORE_FIELD.getPreferredName(), minScore);
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
            return new RerankingQueryPhaseRankShardContext(queries, rankWindowSize());
        }

        @Override
        public QueryPhaseRankCoordinatorContext buildQueryPhaseCoordinatorContext(int size, int from) {
            return new RerankingQueryPhaseRankCoordinatorContext(rankWindowSize());
        }

        @Override
        public RankFeaturePhaseRankShardContext buildRankFeaturePhaseShardContext() {
            return new RerankingRankFeaturePhaseRankShardContext(field);
        }

        @Override
        public RankFeaturePhaseRankCoordinatorContext buildRankFeaturePhaseCoordinatorContext(int size, int from, Client client) {
            return new TestRerankingRankFeaturePhaseRankCoordinatorContext(
                size,
                from,
                rankWindowSize(),
                client,
                inferenceId,
                inferenceText,
                minScore
            );
        }

        @Override
        protected boolean doEquals(RankBuilder other) {
            return other instanceof MockRequestActionBasedRankBuilder
                && Objects.equals(field, ((MockRequestActionBasedRankBuilder) other).field);
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(field);
        }

        @Override
        public String getWriteableName() {
            return "request_action_based_rank";
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_8_15_0;
        }
    }

    public static class ThrowingMockRequestActionBasedRankBuilder extends MockRequestActionBasedRankBuilder {

        public static final ParseField FIELD_FIELD = new ParseField("field");
        public static final ParseField INFERENCE_ID = new ParseField("inference_id");
        public static final ParseField INFERENCE_TEXT = new ParseField("inference_text");
        public static final ParseField THROWING_TYPE_FIELD = new ParseField("throwing-type");
        static final ConstructingObjectParser<ThrowingMockRequestActionBasedRankBuilder, Void> PARSER = new ConstructingObjectParser<>(
            "throwing_request_action_based_rank",
            args -> {
                int rankWindowSize = args[0] == null ? DEFAULT_RANK_WINDOW_SIZE : (int) args[0];
                String field = (String) args[1];
                if (field == null || field.isEmpty()) {
                    throw new IllegalArgumentException("Field cannot be null or empty");
                }
                final String inferenceId = (String) args[2];
                final String inferenceText = (String) args[3];
                final float minScore = (float) args[4];
                String throwingType = (String) args[5];
                return new ThrowingMockRequestActionBasedRankBuilder(
                    rankWindowSize,
                    field,
                    inferenceId,
                    inferenceText,
                    minScore,
                    throwingType
                );
            }
        );

        static {
            PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
            PARSER.declareString(constructorArg(), FIELD_FIELD);
            PARSER.declareString(constructorArg(), INFERENCE_ID);
            PARSER.declareString(constructorArg(), INFERENCE_TEXT);
            PARSER.declareString(constructorArg(), THROWING_TYPE_FIELD);
        }

        public static ThrowingMockRequestActionBasedRankBuilder fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        protected final ThrowingRankBuilderType throwingRankBuilderType;

        public ThrowingMockRequestActionBasedRankBuilder(
            final int rankWindowSize,
            final String field,
            final String inferenceId,
            final String inferenceText,
            final float minScore,
            final String throwingType
        ) {
            super(rankWindowSize, field, inferenceId, inferenceText, minScore);
            this.throwingRankBuilderType = ThrowingRankBuilderType.valueOf(throwingType);
        }

        public ThrowingMockRequestActionBasedRankBuilder(StreamInput in) throws IOException {
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
                return new TestRerankingRankFeaturePhaseRankCoordinatorContext(
                    size,
                    from,
                    rankWindowSize(),
                    client,
                    inferenceId,
                    inferenceText,
                    minScore
                ) {
                    @Override
                    protected TestRerankingActionRequest generateRequest(List<String> docFeatures) {
                        return new TestThrowingRerankingActionRequest(docFeatures);
                    }
                };
            else {
                return super.buildRankFeaturePhaseCoordinatorContext(size, from, client);
            }
        }

        @Override
        public String getWriteableName() {
            return "throwing_request_action_based_rank";
        }
    }
}
