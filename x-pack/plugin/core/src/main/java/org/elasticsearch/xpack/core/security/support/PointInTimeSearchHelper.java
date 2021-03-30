/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ClosePointInTimeAction;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeAction;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.sort.ShardDocSortField;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public final class PointInTimeSearchHelper {

    private static final Logger logger = LogManager.getLogger(PointInTimeSearchHelper.class);
    private static final TimeValue KEEP_ALIVE = TimeValue.timeValueMinutes(1);

    private PointInTimeSearchHelper() {}

    public static <T> void fetchAll(Client client, SearchRequest searchRequest,
                                    Function<SearchHit, T> hitParser,
                                    final ActionListener<Collection<T>> listener) {

        final List<SortBuilder<?>> sorts = searchRequest.source().sorts();
        if (sorts == null || sorts.isEmpty()) {
            searchRequest.source().sort(ShardDocSortField.NAME);
        }

        final OpenPointInTimeRequest openPointInTimeRequest =
            new OpenPointInTimeRequest(searchRequest.indices(), searchRequest.indicesOptions(), KEEP_ALIVE, null, null);

        logger.trace("opening pit for indices: [{}]", Strings.arrayToCommaDelimitedString(searchRequest.indices()));
        executeAsyncWithOrigin(client, SECURITY_ORIGIN, OpenPointInTimeAction.INSTANCE, openPointInTimeRequest,
            ActionListener.wrap(openPointInTimeResponse -> {
                final String pitId = openPointInTimeResponse.getSearchContextId();
                final OriginSettingClient securityOriginClient = new OriginSettingClient(client, SECURITY_ORIGIN);
                getNextPage(new ArrayList<T>(), pitId, null, securityOriginClient, searchRequest, hitParser, listener);
            }, listener::onFailure));
    }

    private static <T> void getNextPage(List<T> results, String pitId, Object[] sortValues,
                                        Client client,
                                        SearchRequest searchRequest,
                                        Function<SearchHit, T> hitParser,
                                        final ActionListener<Collection<T>> listener) {
        assert client instanceof OriginSettingClient : "client should have a specific origin";
        final PointInTimeBuilder pointInTimeBuilder = new PointInTimeBuilder(pitId).setKeepAlive(KEEP_ALIVE);
        if (results.isEmpty()) {  // first page
            assert sortValues == null : "search_after should be null for the first page";
            logger.trace("getting first page with pit: [{}]", pitId);
            searchRequest.source().pointInTimeBuilder(pointInTimeBuilder);
        } else {
            logger.trace("getting next page with pit: [{}] and sort values: [{}]",
                pitId, Strings.arrayToCommaDelimitedString(sortValues));
            searchRequest.source().searchAfter(sortValues).pointInTimeBuilder(pointInTimeBuilder);
        }
        client.threadPool().executor(ThreadPool.Names.SAME).execute(() -> {
            client.execute(SearchAction.INSTANCE, searchRequest,
                ActionListener.wrap(searchResponse -> {
                    processSearchResponse(searchResponse, results, client, searchRequest, hitParser, listener);
                }, listener::onFailure));
        });
    }

    private static <T> void processSearchResponse(SearchResponse searchResponse, List<T> results,
                                                  Client client, SearchRequest searchRequest,
                                                  Function<SearchHit, T> hitParser,
                                                  final ActionListener<Collection<T>> listener) {
        assert client instanceof OriginSettingClient : "client should have a specific origin";
        final SearchHit[] hits = searchResponse.getHits().getHits();
        results.addAll(Arrays.stream(hits).map(hitParser).collect(Collectors.toUnmodifiableList()));
        final String pitId = searchResponse.pointInTimeId();
        // Not using totalHits as it must be interpreted with the relation, and tracking total hits is has cost
        if (hits.length < searchRequest.source().size()) {
            logger.trace("reaching the end of result, total size is [{}]", results.size());
            listener.onResponse(results);
            // Intentionally not return here so point in time is closed async
            closePointInTime(pitId, client);
        } else {
            final Object[] sortValues = hits[hits.length - 1].getSortValues();
            getNextPage(results, pitId, sortValues, client, searchRequest, hitParser, listener);
        }
    }

    private static void closePointInTime(String pitId, Client client) {
        assert client instanceof OriginSettingClient : "client should have a specific origin";
        client.threadPool().executor(ThreadPool.Names.SAME).execute(() -> {
            client.execute(ClosePointInTimeAction.INSTANCE, new ClosePointInTimeRequest(pitId),
                ActionListener.wrap(response -> {
                    logger.trace("pit [{}] closed", pitId);
                }, e -> {
                    logger.warn(new ParameterizedMessage("failed to close [{}]", pitId), e);
                }));
        });
    }
}
