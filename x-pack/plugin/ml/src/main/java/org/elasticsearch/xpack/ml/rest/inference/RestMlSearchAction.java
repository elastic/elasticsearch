/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.search.builder.SearchSourceBuilder.TRACK_TOTAL_HITS_FIELD;
import static org.elasticsearch.search.internal.SearchContext.TRACK_TOTAL_HITS_ACCURATE;
import static org.elasticsearch.search.internal.SearchContext.TRACK_TOTAL_HITS_DISABLED;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction.Request.INFERENCE_CONFIG;

public class RestMlSearchAction extends BaseRestHandler {

    public RestMlSearchAction() {}

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "{index}/_ml_search"), new Route(POST, "{index}/_ml_search"));
    }

    @Override
    public String getName() {
        return "ml_search_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        // This will allow to cancel the search request if the http channel is closed
        RestCancellableNodeClient cancellableNodeClient = new RestCancellableNodeClient(client, restRequest.getHttpChannel());
        MlSearchRequestBuilder request = MlSearchRequestBuilder.parseRestRequest(restRequest);
        return channel -> {
            RestToXContentListener<SearchResponseWithInferenceTime> listener = new RestToXContentListener<>(channel);
            cancellableNodeClient.execute(
                InferTrainedModelDeploymentAction.INSTANCE,
                request.inferenceRequest(),
                ActionListener.wrap(inferenceResponse -> {
                    SearchRequestBuilder searchRequestBuilder = cancellableNodeClient.prepareSearch();
                    request.build(searchRequestBuilder, inferenceResponse.getResults());
                    searchRequestBuilder.execute(ActionListener.wrap(
                        r -> new SearchResponseWithInferenceTime(r.getTook(), TimeValue.timeValueMillis(inferenceResponse.getTookMillis()),r),
                        listener::onFailure)
                    );
                }, listener::onFailure)

            );
        };
    }

    private static class SearchResponseWithInferenceTime implements ToXContentObject {
        private final TimeValue searchTook;
        private final TimeValue inferenceTook;
        private final SearchResponse response;

        public SearchResponseWithInferenceTime(TimeValue searchTook, TimeValue inferenceTook, SearchResponse response) {
            this.searchTook = searchTook;
            this.inferenceTook = inferenceTook;
            this.response = response;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("search_took", searchTook.millis());
            builder.field("inference_took", inferenceTook.millis());
            builder.field("search_response", response);
            builder.endObject();
            return builder;
        }
    }

    public static class MlSearchRequestBuilder {

        static final ParseField KNN_SECTION_FIELD = new ParseField("knn");
        static final ParseField FILTER_FIELD = new ParseField("query");
        private static final ObjectParser<MlSearchRequestBuilder, Void> PARSER = new ObjectParser<>("ml_search");
        static {
            PARSER.declareField(
                MlSearchRequestBuilder::setKnnSearch,
                MlSearchRequestBuilder.KnnSearch::parse,
                KNN_SECTION_FIELD,
                ObjectParser.ValueType.OBJECT
            );
            PARSER.declareFieldArray(
                MlSearchRequestBuilder::setFilter,
                (p, c) -> AbstractQueryBuilder.parseInnerQueryBuilder(p),
                FILTER_FIELD,
                ObjectParser.ValueType.OBJECT_ARRAY
            );
            PARSER.declareField(
                (p, request, c) -> request.setFetchSource(FetchSourceContext.fromXContent(p)),
                SearchSourceBuilder._SOURCE_FIELD,
                ObjectParser.ValueType.OBJECT_ARRAY_BOOLEAN_OR_STRING
            );
            PARSER.declareFieldArray(
                MlSearchRequestBuilder::setFields,
                (p, c) -> FieldAndFormat.fromXContent(p),
                SearchSourceBuilder.FETCH_FIELDS_FIELD,
                ObjectParser.ValueType.OBJECT_ARRAY
            );
            PARSER.declareField(MlSearchRequestBuilder::setTrackTotalHits, (p, c) -> {
                if (p.currentToken() == XContentParser.Token.VALUE_BOOLEAN
                    || (p.currentToken() == XContentParser.Token.VALUE_STRING && Booleans.isBoolean(p.text()))) {
                    return TRACK_TOTAL_HITS_ACCURATE;
                } else {
                    return p.intValue();
                }
            }, TRACK_TOTAL_HITS_FIELD, ObjectParser.ValueType.VALUE);
        }

        public static MlSearchRequestBuilder parseRestRequest(RestRequest restRequest) throws IOException {
            MlSearchRequestBuilder builder = new MlSearchRequestBuilder(Strings.splitStringByCommaToArray(restRequest.param("index")));
            if (restRequest.hasContentOrSourceParam()) {
                try (XContentParser contentParser = restRequest.contentOrSourceParamParser()) {
                    PARSER.parse(contentParser, builder, null);
                }
            }
            return builder;
        }

        private MlSearchRequestBuilder.KnnSearch knnSearch;
        private List<QueryBuilder> filter;
        private FetchSourceContext fetchSource;
        private List<FieldAndFormat> fields;
        private final String[] indices;
        private int trackTotalHits = TRACK_TOTAL_HITS_DISABLED;

        public MlSearchRequestBuilder(String[] indices) {
            this.indices = indices;
        }

        public MlSearchRequestBuilder.KnnSearch getKnnSearch() {
            return knnSearch;
        }

        public MlSearchRequestBuilder setTrackTotalHits(int trackTotalHits) {
            this.trackTotalHits = trackTotalHits;
            return this;
        }

        public MlSearchRequestBuilder setKnnSearch(MlSearchRequestBuilder.KnnSearch knnSearch) {
            this.knnSearch = knnSearch;
            return this;
        }

        public List<QueryBuilder> getFilter() {
            return filter;
        }

        public MlSearchRequestBuilder setFilter(List<QueryBuilder> filter) {
            this.filter = filter;
            return this;
        }

        public FetchSourceContext getFetchSource() {
            return fetchSource;
        }

        public MlSearchRequestBuilder setFetchSource(FetchSourceContext fetchSource) {
            this.fetchSource = fetchSource;
            return this;
        }

        public List<FieldAndFormat> getFields() {
            return fields;
        }

        public MlSearchRequestBuilder setFields(List<FieldAndFormat> fields) {
            this.fields = fields;
            return this;
        }

        public InferTrainedModelDeploymentAction.Request inferenceRequest() {
            return new InferTrainedModelDeploymentAction.Request(
                knnSearch.modelId,
                knnSearch.inferenceConfigUpdate,
                List.of(Map.of("text_field", knnSearch.queryString)),
                TimeValue.MAX_VALUE
            );
        }

        public void build(SearchRequestBuilder builder, InferenceResults inferenceResults) {
            builder.setIndices(indices);

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            if (knnSearch == null) {
                throw new IllegalArgumentException(
                    "missing required [" + KNN_SECTION_FIELD.getPreferredName() + "] section in search body"
                );
            }
            if (inferenceResults instanceof TextEmbeddingResults textEmbeddingResults) {
                KnnVectorQueryBuilder queryBuilder = knnSearch.buildQuery(textEmbeddingResults);
                if (filter != null) {
                    queryBuilder.addFilterQueries(this.filter);
                }
                sourceBuilder.query(queryBuilder);
                sourceBuilder.size(knnSearch.k);

                if (trackTotalHits < 0) {
                    sourceBuilder.trackTotalHits(false);
                } else {
                    sourceBuilder.trackTotalHitsUpTo(trackTotalHits);
                }
                if (fetchSource != null) {
                    sourceBuilder.fetchSource(fetchSource);
                }
                if (fields != null) {
                    for (FieldAndFormat field : fields) {
                        sourceBuilder.fetchField(field);
                    }
                }
                builder.setSource(sourceBuilder);
                return;
            }
            throw new IllegalArgumentException(
                "inference results for model ["
                    + knnSearch.modelId
                    + "] must be text_embedding; provided ["
                    + inferenceResults.getWriteableName()
                    + "]"
            );
        }

        public record KnnSearch(
            String field,
            int k,
            int numCandidates,
            String modelId,
            String queryString,
            Double boost,
            InferenceConfigUpdate inferenceConfigUpdate
        ) {

            private static final int NUM_CANDS_LIMIT = 10000;
            static final ParseField FIELD_FIELD = new ParseField("field");
            static final ParseField K_FIELD = new ParseField("k");
            static final ParseField NUM_CANDS_FIELD = new ParseField("num_candidates");
            static final ParseField MODEL_ID = new ParseField("model_id");
            static final ParseField QUERY_STRING = new ParseField("query_string");

            private static final ConstructingObjectParser<MlSearchRequestBuilder.KnnSearch, Void> PARSER = new ConstructingObjectParser<>(
                "knn",
                args -> new MlSearchRequestBuilder.KnnSearch(
                    (String) args[0],
                    (int) args[1],
                    (int) args[2],
                    (String) args[3],
                    (String) args[4],
                    (Double) args[5],
                    (InferenceConfigUpdate) args[6]
                )
            );

            static {
                PARSER.declareString(constructorArg(), FIELD_FIELD);
                PARSER.declareInt(constructorArg(), K_FIELD);
                PARSER.declareInt(constructorArg(), NUM_CANDS_FIELD);
                PARSER.declareString(constructorArg(), MODEL_ID);
                PARSER.declareString(constructorArg(), QUERY_STRING);
                PARSER.declareDouble(optionalConstructorArg(), AbstractQueryBuilder.BOOST_FIELD);
                PARSER.declareNamedObject(
                    optionalConstructorArg(),
                    ((p, c, name) -> p.namedObject(InferenceConfigUpdate.class, name, c)),
                    INFERENCE_CONFIG
                );
            }

            public static MlSearchRequestBuilder.KnnSearch parse(XContentParser parser) throws IOException {
                return PARSER.parse(parser, null);
            }

            public KnnVectorQueryBuilder buildQuery(TextEmbeddingResults inferenceResults) {
                // We perform validation here instead of the constructor because it makes the errors
                // much clearer. Otherwise, the error message is deeply nested under parsing exceptions.
                if (k < 1) {
                    throw new IllegalArgumentException("[" + K_FIELD.getPreferredName() + "] must be greater than 0");
                }
                if (numCandidates < k) {
                    throw new IllegalArgumentException(
                        "[" + NUM_CANDS_FIELD.getPreferredName() + "] cannot be less than " + "[" + K_FIELD.getPreferredName() + "]"
                    );
                }
                if (numCandidates > NUM_CANDS_LIMIT) {
                    throw new IllegalArgumentException(
                        "[" + NUM_CANDS_FIELD.getPreferredName() + "] cannot exceed [" + NUM_CANDS_LIMIT + "]"
                    );
                }
                float[] queryVector = new float[inferenceResults.getInference().length];
                int i = 0;
                for (double v : inferenceResults.getInference()) {
                    queryVector[i++] = (float) v;
                }

                return new KnnVectorQueryBuilder(field, queryVector, numCandidates);
            }

        }

    }

}
