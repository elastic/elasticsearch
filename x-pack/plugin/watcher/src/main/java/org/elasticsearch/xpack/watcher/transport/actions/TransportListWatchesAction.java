/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.transport.actions.ListWatchesAction;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.ClockHolder;
import org.elasticsearch.xpack.watcher.watch.WatchParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.WATCHER_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportListWatchesAction extends WatcherTransportAction<ListWatchesAction.Request, ListWatchesAction.Response> {

    private final Clock clock;
    private final Client client;
    private final WatchParser parser;

    @Inject
    public TransportListWatchesAction(TransportService transportService, ActionFilters actionFilters, XPackLicenseState licenseState,
                                      ClockHolder clockHolder, Client client, WatchParser parser) {
        super(ListWatchesAction.NAME, transportService, actionFilters, licenseState, ListWatchesAction.Request::new);
        this.clock = clockHolder.clock;
        this.client = client;
        this.parser = parser;
    }

    @Override
    protected void doExecute(ListWatchesAction.Request request, ActionListener<ListWatchesAction.Response> listener) {
        SearchRequest searchRequest = createSearchRequest(request);
        executeAsyncWithOrigin(client.threadPool().getThreadContext(), WATCHER_ORIGIN, searchRequest,
            ActionListener.<SearchResponse>wrap(r -> transformResponse(r, listener), listener::onFailure), client::search);
    }

    SearchRequest createSearchRequest(ListWatchesAction.Request request) {
        SearchRequest searchRequest = new SearchRequest(Watch.INDEX);
        if (request.getFrom() != null) {
            searchRequest.source().from(request.getFrom());
        }
        if (request.getSize() != null) {
            searchRequest.source().size(request.getSize());
        }
        if (request.getQuery() != null) {
            searchRequest.source().query(request.getQuery());
        }
        if (request.getSorts() != null) {
            for (FieldSortBuilder sort : request.getSorts()) {
                searchRequest.source().sort(sort);
            }
        }
        searchRequest.source().trackTotalHits(true);
        searchRequest.source().seqNoAndPrimaryTerm(true);
        return searchRequest;
    }


    void transformResponse(SearchResponse searchResponse, ActionListener<ListWatchesAction.Response> listener) {
        assert searchResponse.getHits().getTotalHits().relation == TotalHits.Relation.EQUAL_TO;
        List<ListWatchesAction.Response.Item> items = Arrays.stream(searchResponse.getHits().getHits())
            .map(this::transformSearchHit)
            .collect(Collectors.toList());
        listener.onResponse(new ListWatchesAction.Response(searchResponse.getHits().getTotalHits().value, items));
    }

    ListWatchesAction.Response.Item transformSearchHit(SearchHit searchHit) {
        ZonedDateTime now = clock.instant().atZone(ZoneOffset.UTC);
        try (XContentBuilder builder = jsonBuilder()) {
            Watch watch = parser.parseWithSecrets(searchHit.getId(), true, searchHit.getSourceRef(), now,
                XContentType.JSON, searchHit.getSeqNo(), searchHit.getPrimaryTerm());
            watch.toXContent(builder, WatcherParams.builder()
                .hideSecrets(true)
                .includeStatus(false)
                .build());
            return new ListWatchesAction.Response.Item(searchHit.getId(), new XContentSource(builder), watch.status(),
                watch.getSourceSeqNo(), watch.getSourcePrimaryTerm());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
