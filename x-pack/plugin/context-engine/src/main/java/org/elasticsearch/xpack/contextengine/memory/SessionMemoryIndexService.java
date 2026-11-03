/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.memory;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.RerankRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.contextengine.ContextEnginePlugin;
import org.elasticsearch.xpack.contextengine.memory.action.ListSessionMemoryAction;
import org.elasticsearch.xpack.contextengine.memory.action.SearchSessionMemoryAction;
import org.elasticsearch.xpack.core.inference.action.RerankAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.inference.RerankRequest.SUPPORTED_RERANK_DATA_TYPES;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.CONTEXT_ENGINE_ORIGIN;
import static org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest.TIMEOUT_NOT_DETERMINED;

/**
 * Reads and writes the session memory data stream on behalf of the Context Engine actions.
 */
public class SessionMemoryIndexService {

    private static final String CONTENT_SEMANTIC_FIELD = SessionMemory.CONTENT_FIELD.getPreferredName() + ".semantic";
    /** Candidates sent to the reranker per requested hit, with a floor so small requests still rerank a useful pool. */
    static final int RERANK_WINDOW_FACTOR = 2;
    static final int MIN_RERANK_WINDOW = 20;

    private final Client clientWithOrigin;
    private final NamedXContentRegistry xContentRegistry;
    private final Predicate<NodeFeature> clusterSupportsFeature;

    public SessionMemoryIndexService(Client client, NamedXContentRegistry xContentRegistry, Predicate<NodeFeature> clusterSupportsFeature) {
        this.clientWithOrigin = new OriginSettingClient(client, CONTEXT_ENGINE_ORIGIN);
        this.xContentRegistry = xContentRegistry;
        this.clusterSupportsFeature = clusterSupportsFeature;
    }

    /**
     * Appends one memory to the data stream, assigning an id when the caller did not supply one.
     */
    public void remember(SessionMemory memory, ActionListener<SessionMemory> listener) {
        try {
            String id = Strings.isNullOrEmpty(memory.id()) ? UUIDs.randomBase64UUID() : memory.id();
            SessionMemory stored = memory.withIdAndTimestamp(id, Instant.now().truncatedTo(ChronoUnit.MILLIS));
            clientWithOrigin.prepareIndex(ContextEnginePlugin.SESSION_MEMORY_DATA_STREAM_NAME)
                .setOpType(DocWriteRequest.OpType.CREATE)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                .setSource(stored.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .execute(listener.delegateFailureAndWrap((l, indexResponse) -> l.onResponse(stored)));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Returns the newest memories matching the request's filters, without any inference.
     */
    public void list(ListSessionMemoryAction.Request request, ActionListener<List<SessionMemory>> listener) {
        try {
            BoolQueryBuilder query = QueryBuilders.boolQuery();
            filters(request.sessionId(), request.userId(), request.agentId(), request.space(), request.aiIndexId()).forEach(query::filter);
            SearchSourceBuilder source = new SearchSourceBuilder().query(query)
                .size(request.size())
                .sort(SessionMemory.TIMESTAMP_FIELD.getPreferredName(), SortOrder.DESC);
            SearchRequest searchRequest = new SearchRequest(ContextEnginePlugin.SESSION_MEMORY_DATA_STREAM_NAME).source(source);
            clientWithOrigin.search(searchRequest, ActionListener.wrap(searchResponse -> {
                List<SessionMemory> memories = new ArrayList<>();
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    memories.add(parseHit(hit));
                }
                listener.onResponse(memories);
            }, e -> {
                if (e instanceof IndexNotFoundException) {
                    listener.onResponse(List.of());
                } else {
                    listener.onFailure(e);
                }
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Two-stage search. The first stage is an {@code rrf} retriever over one {@code knn} query per field, {@code content.semantic}
     * and {@code media}, each embedding the query with {@code inferenceId} and applying the request's filters inside the kNN clause
     * so they narrow the candidates rather than the results. The second stage sends every candidate's text and images to
     * {@code rerankInferenceId}, which scores text and image documents against the query on one scale, so an image-only
     * memory is not ranked below text that merely resembles the query. A memory's score is its best document. The rerank API
     * accepts only text and images, so a PDF query keeps the first stage's order and scores, and a memory with nothing the
     * reranker accepts follows the reranked hits in first-stage order with a zero score.
     */
    public void search(
        SearchSessionMemoryAction.Request request,
        String inferenceId,
        String rerankInferenceId,
        ActionListener<List<SearchSessionMemoryAction.Hit>> listener
    ) {
        try {
            List<QueryBuilder> filters = filters(
                request.sessionId(),
                request.userId(),
                request.agentId(),
                request.space(),
                request.aiIndexId()
            );
            int window = Math.max(request.size() * RERANK_WINDOW_FACTOR, MIN_RERANK_WINDOW);
            SearchSourceBuilder source = parseSource(retrieverSource(request.query(), inferenceId, window, filters));
            SearchRequest searchRequest = new SearchRequest(ContextEnginePlugin.SESSION_MEMORY_DATA_STREAM_NAME).source(source);
            InferenceString query = inferenceString(request.query());
            clientWithOrigin.search(searchRequest, ActionListener.wrap(searchResponse -> {
                List<SearchSessionMemoryAction.Hit> firstStage = new ArrayList<>();
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    firstStage.add(new SearchSessionMemoryAction.Hit(parseHit(hit), hit.getScore()));
                }
                RerankInputs inputs = RerankInputs.of(firstStage.stream().map(SearchSessionMemoryAction.Hit::memory).toList());
                if (SUPPORTED_RERANK_DATA_TYPES.contains(query.dataType()) == false || inputs.documents().isEmpty()) {
                    listener.onResponse(firstStage.size() > request.size() ? firstStage.subList(0, request.size()) : firstStage);
                    return;
                }
                RerankAction.Request rerankRequest = new RerankAction.Request(
                    rerankInferenceId,
                    new RerankRequest(inputs.documents(), query, null, false, null),
                    TIMEOUT_NOT_DETERMINED
                );
                clientWithOrigin.execute(
                    RerankAction.INSTANCE,
                    rerankRequest,
                    listener.delegateFailureAndWrap(
                        (l, response) -> l.onResponse(
                            inputs.rank(((RankedDocsResults) response.getResults()).getRankedDocs(), request.size())
                        )
                    )
                );
            }, e -> {
                if (e instanceof IndexNotFoundException) {
                    listener.onResponse(List.of());
                } else {
                    listener.onFailure(e);
                }
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private static List<QueryBuilder> filters(String sessionId, String userId, String agentId, String space, String aiIndexId) {
        List<QueryBuilder> filters = new ArrayList<>();
        addTermFilter(filters, SessionMemory.SESSION_ID_FIELD, sessionId);
        addTermFilter(filters, SessionMemory.USER_ID_FIELD, userId);
        addTermFilter(filters, SessionMemory.AGENT_ID_FIELD, agentId);
        addTermFilter(filters, SessionMemory.SPACE_FIELD, space);
        addTermFilter(filters, SessionMemory.AI_INDEX_ID_FIELD, aiIndexId);
        return filters;
    }

    private static void addTermFilter(List<QueryBuilder> filters, ParseField field, String value) {
        if (value != null) {
            filters.add(QueryBuilders.termQuery(field.getPreferredName(), value));
        }
    }

    /**
     * The {@code embedding} query vector builder and the {@code rrf} retriever live in plugins that do not export them, so the
     * search source is built as JSON and parsed through the node's registry. Each field gets a {@code standard} retriever around
     * a {@code knn} query rather than a {@code knn} retriever, because only the query is rewritten onto the {@code semantic}
     * fields' embeddings.
     */
    static String retrieverSource(Object input, String inferenceId, int window, List<QueryBuilder> filters) throws IOException {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        {
            builder.field("size", window);
            builder.startObject("retriever");
            {
                builder.startObject("rrf");
                {
                    builder.field("rank_window_size", window);
                    builder.startArray("retrievers");
                    for (String field : List.of(CONTENT_SEMANTIC_FIELD, SessionMemory.MEDIA_FIELD.getPreferredName())) {
                        builder.startObject();
                        {
                            builder.startObject("standard");
                            {
                                builder.startObject("query");
                                knnQuery(builder, field, input, inferenceId, window, filters);
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endArray();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return Strings.toString(builder);
    }

    private static void knnQuery(XContentBuilder builder, String field, Object input, String inferenceId, int k, List<QueryBuilder> filters)
        throws IOException {
        builder.startObject("knn");
        {
            builder.field("field", field);
            builder.field("k", k);
            builder.field("num_candidates", Math.max(100, k * 10));
            builder.startObject("query_vector_builder");
            {
                builder.startObject("embedding");
                {
                    builder.field("inference_id", inferenceId);
                    builder.field("input", input);
                }
                builder.endObject();
            }
            builder.endObject();
            if (filters.isEmpty() == false) {
                builder.startArray("filter");
                for (QueryBuilder filter : filters) {
                    filter.toXContent(builder, ToXContent.EMPTY_PARAMS);
                }
                builder.endArray();
            }
        }
        builder.endObject();
    }

    private SearchSourceBuilder parseSource(String json) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry), json)
        ) {
            return new SearchSourceBuilder().parseXContent(parser, true, clusterSupportsFeature);
        }
    }

    /**
     * A search query or a stored media item as a typed rerank input: text as-is, otherwise the item's {@code type},
     * {@code format} and {@code value}.
     */
    static InferenceString inferenceString(Object input) {
        if (input instanceof String text) {
            return InferenceString.ofText(text);
        }
        Map<?, ?> item = (Map<?, ?>) input;
        Object format = item.get("format");
        return new InferenceString(
            DataType.fromString((String) item.get("type")),
            format == null ? null : DataFormat.fromString((String) format),
            (String) item.get("value")
        );
    }

    /**
     * The rerank documents for a candidate list: a memory's content and each media item the rerank API accepts become one
     * document each, and {@code owners} maps every document back to its memory so the best document decides the memory's score.
     */
    record RerankInputs(List<SessionMemory> candidates, List<InferenceString> documents, List<Integer> owners) {

        static RerankInputs of(List<SessionMemory> candidates) {
            List<InferenceString> documents = new ArrayList<>();
            List<Integer> owners = new ArrayList<>();
            for (int i = 0; i < candidates.size(); i++) {
                SessionMemory memory = candidates.get(i);
                if (Strings.isNullOrEmpty(memory.content()) == false) {
                    documents.add(InferenceString.ofText(memory.content()));
                    owners.add(i);
                }
                for (Map<String, Object> item : memory.media()) {
                    InferenceString document = inferenceString(item);
                    if (SUPPORTED_RERANK_DATA_TYPES.contains(document.dataType())) {
                        documents.add(document);
                        owners.add(i);
                    }
                }
            }
            return new RerankInputs(candidates, documents, owners);
        }

        /**
         * Orders the candidates by their best reranked document, then the candidates the reranker did not score in their
         * first-stage order with a zero score, keeping at most {@code size}.
         */
        List<SearchSessionMemoryAction.Hit> rank(List<RankedDocsResults.RankedDoc> rankedDocs, int size) {
            Float[] best = new Float[candidates.size()];
            for (RankedDocsResults.RankedDoc doc : rankedDocs) {
                int owner = owners.get(doc.index());
                if (best[owner] == null || doc.relevanceScore() > best[owner]) {
                    best[owner] = doc.relevanceScore();
                }
            }
            List<SearchSessionMemoryAction.Hit> hits = new ArrayList<>();
            List<SearchSessionMemoryAction.Hit> unscored = new ArrayList<>();
            for (int i = 0; i < candidates.size(); i++) {
                if (best[i] != null) {
                    hits.add(new SearchSessionMemoryAction.Hit(candidates.get(i), best[i]));
                } else {
                    unscored.add(new SearchSessionMemoryAction.Hit(candidates.get(i), 0f));
                }
            }
            hits.sort(Comparator.comparingDouble(SearchSessionMemoryAction.Hit::score).reversed());
            hits.addAll(unscored);
            return hits.size() > size ? List.copyOf(hits.subList(0, size)) : hits;
        }
    }

    private static SessionMemory parseHit(SearchHit hit) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, hit.getSourceRef().streamInput())
        ) {
            return SessionMemory.fromXContent(parser);
        }
    }
}
