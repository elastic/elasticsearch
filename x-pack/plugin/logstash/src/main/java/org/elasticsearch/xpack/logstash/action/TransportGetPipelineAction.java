/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logstash.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.logstash.Logstash;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.LOGSTASH_MANAGEMENT_ORIGIN;

public class TransportGetPipelineAction extends HandledTransportAction<GetPipelineRequest, GetPipelineResponse> {

    private static final Logger logger = LogManager.getLogger(TransportGetPipelineAction.class);

    private static final Integer SIZE = 10000;

    private static final String WILDCARD = "*";

    private static final Pattern WILDCARD_PATTERN = Pattern.compile("[^*]+|(\\*)");

    private final Client client;

    @Inject
    public TransportGetPipelineAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(GetPipelineAction.NAME, transportService, actionFilters, GetPipelineRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.client = new OriginSettingClient(client, LOGSTASH_MANAGEMENT_ORIGIN);
    }

    @Override
    protected void doExecute(Task task, GetPipelineRequest request, ActionListener<GetPipelineResponse> listener) {
        final Set<String> explicitPipelineIds = request.ids()
            .stream()
            .filter(pipeline -> pipeline.contains(WILDCARD) == false)
            .collect(Collectors.toSet());

        final Set<Pattern> wildcardPipelinePatterns = request.ids()
            .stream()
            .filter(pipeline -> pipeline.contains(WILDCARD))
            .map(TransportGetPipelineAction::toWildcardPipelineIdPattern)
            .map(Pattern::compile)
            .collect(Collectors.toSet());

        if (explicitPipelineIds.size() > 0 && wildcardPipelinePatterns.size() == 0) {
            getPipelinesByIds(explicitPipelineIds, listener);
            return;
        }

        client.prepareSearch(Logstash.LOGSTASH_CONCRETE_INDEX_NAME)
            .setSource(
                SearchSourceBuilder.searchSource().fetchSource(true).query(QueryBuilders.matchAllQuery()).size(SIZE).trackTotalHits(true)
            )
            .setScroll(TimeValue.timeValueMinutes(1L))
            .execute(ActionListener.wrap(searchResponse -> {
                final int numHits = Math.toIntExact(searchResponse.getHits().getTotalHits().value());
                final Map<String, BytesReference> pipelineSources = Maps.newMapWithExpectedSize(numHits);
                final Consumer<SearchResponse> clearScroll = (response) -> {
                    if (response != null && response.getScrollId() != null) {
                        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                        clearScrollRequest.addScrollId(response.getScrollId());
                        client.clearScroll(
                            clearScrollRequest,
                            ActionListener.wrap(
                                (r) -> {},
                                e -> logger.warn(() -> "clear scroll failed for scroll id [" + response.getScrollId() + "]", e)
                            )
                        );
                    }
                };
                handleFilteringSearchResponse(
                    searchResponse,
                    pipelineSources,
                    explicitPipelineIds,
                    wildcardPipelinePatterns,
                    0,
                    clearScroll,
                    listener
                );
            }, e -> handleFailure(e, listener)));
    }

    private void getPipelinesByIds(Set<String> ids, ActionListener<GetPipelineResponse> listener) {
        client.prepareMultiGet().addIds(Logstash.LOGSTASH_CONCRETE_INDEX_NAME, ids).execute(ActionListener.wrap(mGetResponse -> {
            logFailures(mGetResponse);
            listener.onResponse(
                new GetPipelineResponse(
                    Arrays.stream(mGetResponse.getResponses())
                        .filter(itemResponse -> itemResponse.isFailed() == false)
                        .map(MultiGetItemResponse::getResponse)
                        .filter(GetResponse::isExists)
                        .collect(Collectors.toMap(GetResponse::getId, GetResponse::getSourceAsBytesRef))
                )
            );
        }, e -> handleFailure(e, listener)));
    }

    private static void handleFailure(Exception e, ActionListener<GetPipelineResponse> listener) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        if (cause instanceof IndexNotFoundException) {
            listener.onResponse(new GetPipelineResponse(Map.of()));
        } else {
            listener.onFailure(e);
        }
    }

    private void handleFilteringSearchResponse(
        SearchResponse searchResponse,
        Map<String, BytesReference> pipelineSources,
        Set<String> explicitPipelineIds,
        Set<Pattern> wildcardPipelinePatterns,
        int numberOfHitsSeenPreviously,
        Consumer<SearchResponse> clearScroll,
        ActionListener<GetPipelineResponse> listener
    ) {
        int numberOfHitsSeenSoFar = numberOfHitsSeenPreviously + searchResponse.getHits().getHits().length;
        if (numberOfHitsSeenSoFar > searchResponse.getHits().getTotalHits().value()) {
            clearScroll.accept(searchResponse);
            listener.onFailure(
                new IllegalStateException(
                    "scrolling returned more hits ["
                        + numberOfHitsSeenSoFar
                        + "] than expected ["
                        + searchResponse.getHits().getTotalHits().value()
                        + "] so bailing out to prevent unbounded "
                        + "memory consumption."
                )
            );
        }

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            if (explicitPipelineIds.isEmpty() && wildcardPipelinePatterns.isEmpty()) {
                pipelineSources.put(hit.getId(), hit.getSourceRef());
                continue;
            }

            // take if ID is in request IDs set
            if (explicitPipelineIds.contains(hit.getId())) {
                pipelineSources.put(hit.getId(), hit.getSourceRef());
                continue;
            }
            // take if id matches request wildcard pattern
            if (wildcardPipelinePatterns.stream().anyMatch(pattern -> pattern.matcher(hit.getId()).matches())) {
                pipelineSources.put(hit.getId(), hit.getSourceRef());
            }
        }

        if (numberOfHitsSeenSoFar == searchResponse.getHits().getTotalHits().value()) {
            clearScroll.accept(searchResponse);
            listener.onResponse(new GetPipelineResponse(pipelineSources));
        } else {
            client.prepareSearchScroll(searchResponse.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(1L))
                .execute(
                    listener.delegateFailureAndWrap(
                        (delegate, searchResponse1) -> handleFilteringSearchResponse(
                            searchResponse1,
                            pipelineSources,
                            explicitPipelineIds,
                            wildcardPipelinePatterns,
                            numberOfHitsSeenSoFar,
                            clearScroll,
                            delegate
                        )
                    )
                );
        }
    }

    private static void logFailures(MultiGetResponse multiGetResponse) {
        List<String> ids = Arrays.stream(multiGetResponse.getResponses())
            .filter(MultiGetItemResponse::isFailed)
            .filter(itemResponse -> itemResponse.getFailure() != null)
            .map(itemResponse -> itemResponse.getFailure().getId())
            .collect(Collectors.toList());
        if (ids.isEmpty() == false) {
            logger.info("Could not retrieve logstash pipelines with ids: {}", ids);
        }
    }

    private static String toWildcardPipelineIdPattern(String wildcardPipelineId) {
        Matcher matcher = WILDCARD_PATTERN.matcher(wildcardPipelineId);
        StringBuilder stringBuilder = new StringBuilder();
        while (matcher.find()) {
            if (matcher.group(1) != null) {
                matcher.appendReplacement(stringBuilder, ".*");
            } else {
                matcher.appendReplacement(stringBuilder, "\\\\Q" + matcher.group(0) + "\\\\E");
            }
        }
        matcher.appendTail(stringBuilder);
        return stringBuilder.toString();
    }
}
