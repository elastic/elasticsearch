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
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.contextengine.ContextEnginePlugin;
import org.elasticsearch.xpack.contextengine.memory.action.ListSessionMemoryAction;
import org.elasticsearch.xpack.contextengine.memory.action.SearchSessionMemoryAction;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.CONTEXT_ENGINE_ORIGIN;

/**
 * Reads and writes the session memory data stream on behalf of the Context Engine actions.
 */
public class SessionMemoryIndexService {

    private final Client clientWithOrigin;

    public SessionMemoryIndexService(Client client) {
        this.clientWithOrigin = new OriginSettingClient(client, CONTEXT_ENGINE_ORIGIN);
    }

    /**
     * Appends one memory to the data stream, assigning an id when the caller did not supply one.
     */
    public void remember(SessionMemory memory, ActionListener<SessionMemory> listener) {
        try {
            String id = Strings.isNullOrEmpty(memory.id()) ? UUIDs.randomBase64UUID() : memory.id();
            SessionMemory stored = memory.withIdAndTimestamp(id, Instant.now().truncatedTo(ChronoUnit.MILLIS));
            clientWithOrigin.prepareIndex(ContextEnginePlugin.SESSION_MEMORY_DATA_STREAM_NAME)
                .setId(id)
                .setOpType(DocWriteRequest.OpType.CREATE)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                .setSource(stored.toXContent(jsonBuilder(), SessionMemory.STORED_PARAMS))
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
                .sort(SessionMemory.TIMESTAMP_FIELD.getPreferredName(), SortOrder.DESC)
                .sort(SessionMemory.ID_FIELD.getPreferredName(), SortOrder.ASC);
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
     * Runs one kNN search for the query over the {@code inputs} field, which holds a memory's content and media in a single
     * embedding space, embedding the query with {@code inferenceId}. A memory is scored by its best-matching input. The
     * request's filters are applied inside the kNN clause so they narrow the candidates rather than the results.
     */
    public void search(
        SearchSessionMemoryAction.Request request,
        String inferenceId,
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
            SearchSourceBuilder source = new SearchSourceBuilder().query(knnQuery(request.query(), inferenceId, request.size(), filters))
                .size(request.size());
            SearchRequest searchRequest = new SearchRequest(ContextEnginePlugin.SESSION_MEMORY_DATA_STREAM_NAME).source(source);
            clientWithOrigin.search(searchRequest, ActionListener.wrap(searchResponse -> {
                List<SearchSessionMemoryAction.Hit> hits = new ArrayList<>();
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    hits.add(new SearchSessionMemoryAction.Hit(parseHit(hit), hit.getScore()));
                }
                listener.onResponse(hits);
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
     * The {@code embedding} query vector builder lives in the inference plugin, which does not export it, so the kNN clause is
     * built as JSON and parsed on the coordinating node through a wrapper query.
     */
    static QueryBuilder knnQuery(Object input, String inferenceId, int k, List<QueryBuilder> filters) throws IOException {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        {
            builder.startObject("knn");
            {
                builder.field("field", SessionMemory.INPUTS_FIELD.getPreferredName());
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
        builder.endObject();
        return QueryBuilders.wrapperQuery(Strings.toString(builder));
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
