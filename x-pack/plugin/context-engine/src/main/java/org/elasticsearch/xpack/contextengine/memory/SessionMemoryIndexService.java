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
import org.elasticsearch.index.query.DisMaxQueryBuilder;
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

    private static final String CONTENT_SEMANTIC_FIELD = SessionMemory.CONTENT_FIELD.getPreferredName() + ".semantic";

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
                .setOpType(DocWriteRequest.OpType.CREATE)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                .setSource(stored.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .execute(listener.delegateFailureAndWrap((l, indexResponse) -> l.onResponse(stored)));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Returns the newest memories for a session, optionally narrowed to a space and an AI index.
     */
    public void getBySession(String sessionId, String space, String aiIndexId, int size, ActionListener<List<SessionMemory>> listener) {
        try {
            BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(SessionMemory.SESSION_ID_FIELD.getPreferredName(), sessionId));
            if (space != null) {
                query.filter(QueryBuilders.termQuery(SessionMemory.SPACE_FIELD.getPreferredName(), space));
            }
            if (aiIndexId != null) {
                query.filter(QueryBuilders.termQuery(SessionMemory.AI_INDEX_ID_FIELD.getPreferredName(), aiIndexId));
            }
            SearchSourceBuilder source = new SearchSourceBuilder().query(query)
                .size(size)
                .sort(SessionMemory.TIMESTAMP_FIELD.getPreferredName(), SortOrder.DESC);
            SearchRequest request = new SearchRequest(ContextEnginePlugin.SESSION_MEMORY_DATA_STREAM_NAME).source(source);
            clientWithOrigin.search(request, ActionListener.wrap(searchResponse -> {
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
     * Runs a kNN search for the query over both {@code content.semantic} and {@code media}, embedding it with {@code inferenceId},
     * and applies the request's filters inside each kNN clause so they narrow the candidates rather than the results. The two
     * clauses are combined with {@code dis_max} so a memory is scored by its best-matching field rather than the sum of both,
     * which would otherwise rank every memory carrying media above text-only memories.
     */
    public void search(
        SearchSessionMemoryAction.Request request,
        String inferenceId,
        ActionListener<List<SearchSessionMemoryAction.Hit>> listener
    ) {
        try {
            List<QueryBuilder> filters = new ArrayList<>();
            addTermFilter(filters, SessionMemory.SESSION_ID_FIELD, request.sessionId());
            addTermFilter(filters, SessionMemory.USER_ID_FIELD, request.userId());
            addTermFilter(filters, SessionMemory.AGENT_ID_FIELD, request.agentId());
            addTermFilter(filters, SessionMemory.SPACE_FIELD, request.space());
            addTermFilter(filters, SessionMemory.AI_INDEX_ID_FIELD, request.aiIndexId());

            DisMaxQueryBuilder query = QueryBuilders.disMaxQuery();
            for (String field : List.of(CONTENT_SEMANTIC_FIELD, SessionMemory.MEDIA_FIELD.getPreferredName())) {
                query.add(knnQuery(field, request.query(), inferenceId, request.size(), filters));
            }
            SearchSourceBuilder source = new SearchSourceBuilder().query(query).size(request.size());
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

    private static void addTermFilter(List<QueryBuilder> filters, ParseField field, String value) {
        if (value != null) {
            filters.add(QueryBuilders.termQuery(field.getPreferredName(), value));
        }
    }

    /**
     * The {@code embedding} query vector builder lives in the inference plugin, which does not export it, so the kNN clause is
     * built as JSON and parsed on the coordinating node through a wrapper query.
     */
    private static QueryBuilder knnQuery(String field, Object input, String inferenceId, int size, List<QueryBuilder> filters)
        throws IOException {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        {
            builder.startObject("knn");
            {
                builder.field("field", field);
                builder.field("k", size);
                builder.field("num_candidates", Math.max(100, size * 10));
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
