/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.events;

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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.contextengine.ContextEnginePlugin;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.CONTEXT_ENGINE_ORIGIN;

/**
 * Reads and writes the session events data stream on behalf of the Context Engine actions.
 */
public class SessionEventsIndexService {

    private final Client clientWithOrigin;

    public SessionEventsIndexService(Client client) {
        this.clientWithOrigin = new OriginSettingClient(client, CONTEXT_ENGINE_ORIGIN);
    }

    /**
     * Appends one event to the data stream, assigning an id when the caller did not supply one.
     */
    public void saveEvent(SessionEvent event, ActionListener<SessionEvent> listener) {
        try {
            String id = Strings.isNullOrEmpty(event.id()) ? UUIDs.randomBase64UUID() : event.id();
            SessionEvent stored = event.withIdAndTimestamp(id, Instant.now().truncatedTo(ChronoUnit.MILLIS));
            clientWithOrigin.prepareIndex(ContextEnginePlugin.SESSION_EVENTS_DATA_STREAM_NAME)
                .setOpType(DocWriteRequest.OpType.CREATE)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                .setSource(stored.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .execute(listener.delegateFailureAndWrap((l, indexResponse) -> l.onResponse(stored)));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Returns the newest events for a session, optionally narrowed to a space and an AI index.
     */
    public void getBySession(String sessionId, String space, String aiIndexId, int size, ActionListener<List<SessionEvent>> listener) {
        try {
            BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(SessionEvent.SESSION_ID_FIELD.getPreferredName(), sessionId));
            if (space != null) {
                query.filter(QueryBuilders.termQuery(SessionEvent.SPACE_FIELD.getPreferredName(), space));
            }
            if (aiIndexId != null) {
                query.filter(QueryBuilders.termQuery(SessionEvent.AI_INDEX_IDS_FIELD.getPreferredName(), aiIndexId));
            }
            SearchSourceBuilder source = new SearchSourceBuilder().query(query)
                .size(size)
                .sort(SessionEvent.TIMESTAMP_FIELD.getPreferredName(), SortOrder.DESC)
                .sort(SessionEvent.ID_FIELD.getPreferredName(), SortOrder.ASC);
            SearchRequest request = new SearchRequest(ContextEnginePlugin.SESSION_EVENTS_DATA_STREAM_NAME).source(source);
            clientWithOrigin.search(request, ActionListener.wrap(searchResponse -> {
                List<SessionEvent> events = new ArrayList<>();
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    events.add(parseHit(hit));
                }
                listener.onResponse(events);
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

    private static SessionEvent parseHit(SearchHit hit) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, hit.getSourceRef().streamInput())
        ) {
            return SessionEvent.fromXContent(parser);
        }
    }
}
