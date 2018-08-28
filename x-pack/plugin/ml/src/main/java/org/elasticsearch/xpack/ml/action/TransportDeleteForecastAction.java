/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.DeleteForecastAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.results.Forecast;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats.ForecastRequestStatus;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

import java.util.EnumSet;
import java.util.Set;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;


public class TransportDeleteForecastAction extends HandledTransportAction<DeleteForecastAction.Request,
    AcknowledgedResponse> {

    private static final String RESULTS_INDEX_PATTERN = AnomalyDetectorsIndex.jobResultsIndexPrefix() + "*";
    private final Client client;
    private final JobResultsProvider jobResultsProvider;
    private static final Set<ForecastRequestStatus> DELETABLE_STATUSES =
        EnumSet.of(ForecastRequestStatus.FINISHED, ForecastRequestStatus.FAILED);

    @Inject
    public TransportDeleteForecastAction(Settings settings, TransportService transportService, ActionFilters actionFilters, Client client,
                                         JobResultsProvider jobResultsProvider) {
        super(settings, DeleteForecastAction.NAME, transportService, actionFilters,
            DeleteForecastAction.Request::new);
        this.client = client;
        this.jobResultsProvider = jobResultsProvider;
    }

    @Override
    protected void doExecute(Task task, DeleteForecastAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        final String forecastId = request.getForecastId();
        final String jobId = request.getJobId();
        jobResultsProvider.getForecastRequestStats(jobId, forecastId, (forecastRequestStats) -> {
            if (forecastRequestStats == null) {
                listener.onFailure(
                    new ResourceNotFoundException(Messages.getMessage(Messages.REST_NO_SUCH_FORECAST, forecastId, jobId)));
                return;
            }

            if (DELETABLE_STATUSES.contains(forecastRequestStats.getStatus())) {
                DeleteByQueryRequest deleteByQueryRequest = buildDeleteByQuery(jobId, forecastId);
                executeAsyncWithOrigin(client, ML_ORIGIN, DeleteByQueryAction.INSTANCE, deleteByQueryRequest, ActionListener.wrap(
                    response -> {
                        if (response.getDeleted() > 0) {
                            logger.info("Deleted forecast [{}] from job [{}]", forecastId, jobId);
                            listener.onResponse(new AcknowledgedResponse(true));
                        } else {
                            listener.onFailure(
                                new ResourceNotFoundException(Messages.getMessage(Messages.REST_NO_SUCH_FORECAST, forecastId, jobId)));
                        }
                    },
                    listener::onFailure));
            } else {
                 listener.onFailure(
                     ExceptionsHelper.conflictStatusException(Messages.getMessage(Messages.REST_BAD_FORECAST_STATE, forecastId, jobId)));
            }
        }, listener::onFailure);
    }

    private DeleteByQueryRequest buildDeleteByQuery(String jobId, String forecastId) {
        SearchRequest searchRequest = new SearchRequest();
        // We need to create the DeleteByQueryRequest before we modify the SearchRequest
        // because the constructor of the former wipes the latter
        DeleteByQueryRequest request = new DeleteByQueryRequest(searchRequest);

        searchRequest.indices(RESULTS_INDEX_PATTERN);
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
            .minimumShouldMatch(1)
            .must(QueryBuilders.termsQuery(Result.RESULT_TYPE.getPreferredName(),
                ForecastRequestStats.RESULT_TYPE_VALUE,
                Forecast.RESULT_TYPE_VALUE))
            .should(QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery(Job.ID.getPreferredName(), jobId))
                .must(QueryBuilders.termQuery(Forecast.FORECAST_ID.getPreferredName(), forecastId)));
        QueryBuilder query = QueryBuilders.boolQuery().filter(boolQuery);
        searchRequest.source(new SearchSourceBuilder().query(query));
        return request;
    }
}
