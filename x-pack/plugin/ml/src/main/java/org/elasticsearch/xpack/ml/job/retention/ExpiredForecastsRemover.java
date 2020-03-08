/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.results.Forecast;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Removes up to {@link #MAX_FORECASTS} forecasts (stats + forecasts docs) that have expired.
 * A forecast is deleted if its expiration timestamp is earlier
 * than the start of the current day (local time-zone).
 *
 * This is expected to be used by actions requiring admin rights. Thus,
 * it is also expected that the provided client will be a client with the
 * ML origin so that permissions to manage ML indices are met.
 */
public class ExpiredForecastsRemover implements MlDataRemover {

    private static final Logger LOGGER = LogManager.getLogger(ExpiredForecastsRemover.class);
    private static final int MAX_FORECASTS = 10000;
    private static final String RESULTS_INDEX_PATTERN =  AnomalyDetectorsIndex.jobResultsIndexPrefix() + "*";

    private final OriginSettingClient client;
    private final ThreadPool threadPool;
    private final long cutoffEpochMs;

    public ExpiredForecastsRemover(OriginSettingClient client, ThreadPool threadPool) {
        this.client = Objects.requireNonNull(client);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.cutoffEpochMs = Instant.now(Clock.systemDefaultZone()).toEpochMilli();
    }

    @Override
    public void remove(ActionListener<Boolean> listener, Supplier<Boolean> isTimedOutSupplier) {
        LOGGER.debug("Removing forecasts that expire before [{}]", cutoffEpochMs);
        ActionListener<SearchResponse> forecastStatsHandler = ActionListener.wrap(
                searchResponse -> deleteForecasts(searchResponse, listener, isTimedOutSupplier),
                e -> listener.onFailure(new ElasticsearchException("An error occurred while searching forecasts to delete", e)));

        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(Result.RESULT_TYPE.getPreferredName(), ForecastRequestStats.RESULT_TYPE_VALUE))
                .filter(QueryBuilders.existsQuery(ForecastRequestStats.EXPIRY_TIME.getPreferredName())));
        source.size(MAX_FORECASTS);
        source.trackTotalHits(true);

        // _doc is the most efficient sort order and will also disable scoring
        source.sort(ElasticsearchMappings.ES_DOC);

        SearchRequest searchRequest = new SearchRequest(RESULTS_INDEX_PATTERN);
        searchRequest.source(source);
        client.execute(SearchAction.INSTANCE, searchRequest, new ThreadedActionListener<>(LOGGER, threadPool,
                MachineLearning.UTILITY_THREAD_POOL_NAME, forecastStatsHandler, false));
    }

    private void deleteForecasts(SearchResponse searchResponse, ActionListener<Boolean> listener, Supplier<Boolean> isTimedOutSupplier) {
        List<ForecastRequestStats> forecastsToDelete;
        try {
            forecastsToDelete = findForecastsToDelete(searchResponse);
        } catch (IOException e) {
            listener.onFailure(e);
            return;
        }

        if (isTimedOutSupplier.get()) {
            listener.onResponse(false);
            return;
        }

        DeleteByQueryRequest request = buildDeleteByQuery(forecastsToDelete);
        client.execute(DeleteByQueryAction.INSTANCE, request, new ActionListener<BulkByScrollResponse>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                try {
                    if (bulkByScrollResponse.getDeleted() > 0) {
                        LOGGER.info("Deleted [{}] documents corresponding to [{}] expired forecasts",
                                bulkByScrollResponse.getDeleted(), forecastsToDelete.size());
                    }
                    listener.onResponse(true);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(new ElasticsearchException("Failed to remove expired forecasts", e));
            }
        });
    }

    private List<ForecastRequestStats> findForecastsToDelete(SearchResponse searchResponse) throws IOException {
        List<ForecastRequestStats> forecastsToDelete = new ArrayList<>();

        SearchHits hits = searchResponse.getHits();
        if (hits.getTotalHits().value > MAX_FORECASTS) {
            LOGGER.info("More than [{}] forecasts were found. This run will only delete [{}] of them", MAX_FORECASTS, MAX_FORECASTS);
        }

        for (SearchHit hit : hits.getHits()) {
            try (InputStream stream = hit.getSourceRef().streamInput();
                 XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(
                         NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
                ForecastRequestStats forecastRequestStats = ForecastRequestStats.LENIENT_PARSER.apply(parser, null);
                if (forecastRequestStats.getExpiryTime().toEpochMilli() < cutoffEpochMs) {
                    forecastsToDelete.add(forecastRequestStats);
                }
            }
        }
        return forecastsToDelete;
    }

    private DeleteByQueryRequest buildDeleteByQuery(List<ForecastRequestStats> forecastsToDelete) {
        DeleteByQueryRequest request = new DeleteByQueryRequest();
        request.setSlices(AbstractBulkByScrollRequest.AUTO_SLICES);

        request.indices(RESULTS_INDEX_PATTERN);
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
        boolQuery.must(QueryBuilders.termsQuery(Result.RESULT_TYPE.getPreferredName(),
                ForecastRequestStats.RESULT_TYPE_VALUE, Forecast.RESULT_TYPE_VALUE));
        for (ForecastRequestStats forecastToDelete : forecastsToDelete) {
            boolQuery.should(QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery(Job.ID.getPreferredName(), forecastToDelete.getJobId()))
                    .must(QueryBuilders.termQuery(Forecast.FORECAST_ID.getPreferredName(), forecastToDelete.getForecastId())));
        }
        QueryBuilder query = QueryBuilders.boolQuery().filter(boolQuery);
        request.setQuery(query);

        // _doc is the most efficient sort order and will also disable scoring
        request.getSearchRequest().source().sort(ElasticsearchMappings.ES_DOC);

        return request;
    }
}
