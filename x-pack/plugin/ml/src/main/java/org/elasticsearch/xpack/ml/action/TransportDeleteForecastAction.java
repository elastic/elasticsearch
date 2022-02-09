/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.DeleteForecastAction;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.results.Forecast;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats.ForecastRequestStatus;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.utils.QueryBuilderHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportDeleteForecastAction extends HandledTransportAction<DeleteForecastAction.Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDeleteForecastAction.class);

    private final Client client;
    private final ClusterService clusterService;
    private static final int MAX_FORECAST_TO_SEARCH = 10_000;

    private static final Set<String> DELETABLE_STATUSES = Stream.of(ForecastRequestStatus.FINISHED, ForecastRequestStatus.FAILED)
        .map(ForecastRequestStatus::toString)
        .collect(toSet());

    @Inject
    public TransportDeleteForecastAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService
    ) {
        super(DeleteForecastAction.NAME, transportService, actionFilters, DeleteForecastAction.Request::new);
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, DeleteForecastAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        final String jobId = request.getJobId();

        final String forecastsExpression = request.getForecastId();
        final String[] forecastIds = Strings.splitStringByCommaToArray(forecastsExpression);

        ActionListener<SearchResponse> forecastStatsHandler = ActionListener.wrap(
            searchResponse -> deleteForecasts(searchResponse, request, listener),
            e -> handleFailure(e, request, listener)
        );

        BoolQueryBuilder query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery(Result.RESULT_TYPE.getPreferredName(), ForecastRequestStats.RESULT_TYPE_VALUE));
        QueryBuilderHelper.buildTokenFilterQuery(Forecast.FORECAST_ID.getPreferredName(), forecastIds).ifPresent(query::filter);
        SearchSourceBuilder source = new SearchSourceBuilder().size(MAX_FORECAST_TO_SEARCH)
            // We only need forecast id and status, there is no need fetching the whole source
            .fetchSource(false)
            .docValueField(ForecastRequestStats.FORECAST_ID.getPreferredName())
            .docValueField(ForecastRequestStats.STATUS.getPreferredName())
            .query(query);
        SearchRequest searchRequest = new SearchRequest(AnomalyDetectorsIndex.jobResultsAliasedName(jobId)).source(source);

        executeAsyncWithOrigin(client, ML_ORIGIN, SearchAction.INSTANCE, searchRequest, forecastStatsHandler);
    }

    static List<String> extractForecastIds(SearchHit[] forecastsToDelete, JobState jobState, String jobId) {
        List<String> forecastIds = new ArrayList<>(forecastsToDelete.length);
        List<String> badStatusForecastIds = new ArrayList<>();
        for (SearchHit hit : forecastsToDelete) {
            String forecastId = hit.field(ForecastRequestStats.FORECAST_ID.getPreferredName()).getValue();
            String forecastStatus = hit.field(ForecastRequestStats.STATUS.getPreferredName()).getValue();
            if (DELETABLE_STATUSES.contains(forecastStatus)) {
                forecastIds.add(forecastId);
            } else {
                badStatusForecastIds.add(forecastId);
            }
        }
        if (badStatusForecastIds.size() > 0 && JobState.OPENED.equals(jobState)) {
            throw ExceptionsHelper.conflictStatusException(
                Messages.getMessage(Messages.REST_CANNOT_DELETE_FORECAST_IN_CURRENT_STATE, badStatusForecastIds, jobId)
            );
        }
        return forecastIds;
    }

    private void deleteForecasts(
        SearchResponse searchResponse,
        DeleteForecastAction.Request request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        final String jobId = request.getJobId();
        SearchHits forecastsToDelete = searchResponse.getHits();

        if (forecastsToDelete.getHits().length == 0) {
            if (Strings.isAllOrWildcard(request.getForecastId()) && request.isAllowNoForecasts()) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            } else {
                listener.onFailure(
                    new ResourceNotFoundException(Messages.getMessage(Messages.REST_NO_SUCH_FORECAST, request.getForecastId(), jobId))
                );
            }
            return;
        }
        final ClusterState state = clusterService.state();
        PersistentTasksCustomMetadata persistentTasks = state.metadata().custom(PersistentTasksCustomMetadata.TYPE);
        JobState jobState = MlTasks.getJobState(jobId, persistentTasks);
        final List<String> forecastIds;
        try {
            forecastIds = extractForecastIds(forecastsToDelete.getHits(), jobState, jobId);
        } catch (ElasticsearchException ex) {
            listener.onFailure(ex);
            return;
        }

        DeleteByQueryRequest deleteByQueryRequest = buildDeleteByQuery(jobId, forecastIds);
        executeAsyncWithOrigin(client, ML_ORIGIN, DeleteByQueryAction.INSTANCE, deleteByQueryRequest, ActionListener.wrap(response -> {
            if (response.isTimedOut()) {
                listener.onFailure(
                    new TimeoutException(
                        "Delete request timed out. Successfully deleted "
                            + response.getDeleted()
                            + " forecast documents from job ["
                            + jobId
                            + "]"
                    )
                );
                return;
            }
            if ((response.getBulkFailures().isEmpty() && response.getSearchFailures().isEmpty()) == false) {
                Tuple<RestStatus, Throwable> statusAndReason = getStatusAndReason(response);
                listener.onFailure(
                    new ElasticsearchStatusException(statusAndReason.v2().getMessage(), statusAndReason.v1(), statusAndReason.v2())
                );
                return;
            }
            logger.info("Deleted forecast(s) [{}] from job [{}]", forecastIds, jobId);
            listener.onResponse(AcknowledgedResponse.TRUE);
        }, e -> handleFailure(e, request, listener)));
    }

    private static Tuple<RestStatus, Throwable> getStatusAndReason(final BulkByScrollResponse response) {
        RestStatus status = RestStatus.OK;
        Throwable reason = new Exception("Unknown error");
        // Getting the max RestStatus is sort of arbitrary, would the user care about 5xx over 4xx?
        // Unsure of a better way to return an appropriate and possibly actionable cause to the user.
        for (BulkItemResponse.Failure failure : response.getBulkFailures()) {
            if (failure.getStatus().getStatus() > status.getStatus()) {
                status = failure.getStatus();
                reason = failure.getCause();
            }
        }

        for (ScrollableHitSource.SearchFailure failure : response.getSearchFailures()) {
            RestStatus failureStatus = org.elasticsearch.ExceptionsHelper.status(failure.getReason());
            if (failureStatus.getStatus() > status.getStatus()) {
                status = failureStatus;
                reason = failure.getReason();
            }
        }
        return new Tuple<>(status, reason);
    }

    private DeleteByQueryRequest buildDeleteByQuery(String jobId, List<String> forecastsToDelete) {
        BoolQueryBuilder innerBoolQuery = QueryBuilders.boolQuery()
            .must(
                QueryBuilders.termsQuery(
                    Result.RESULT_TYPE.getPreferredName(),
                    ForecastRequestStats.RESULT_TYPE_VALUE,
                    Forecast.RESULT_TYPE_VALUE
                )
            )
            .must(QueryBuilders.termsQuery(Forecast.FORECAST_ID.getPreferredName(), forecastsToDelete));
        QueryBuilder query = QueryBuilders.boolQuery().filter(innerBoolQuery);

        // We want *all* of the docs to be deleted. Hence, we rely on the default value of max_docs.
        return new DeleteByQueryRequest().setAbortOnVersionConflict(false) // since these documents are not updated, a conflict just means
                                                                           // it was deleted previously
            .setSlices(AbstractBulkByScrollRequest.AUTO_SLICES)
            .indices(AnomalyDetectorsIndex.jobResultsAliasedName(jobId))
            .setQuery(query)
            .setRefresh(true);
    }

    private static void handleFailure(Exception e, DeleteForecastAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
            if (request.isAllowNoForecasts() && Strings.isAllOrWildcard(request.getForecastId())) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            } else {
                listener.onFailure(
                    new ResourceNotFoundException(
                        Messages.getMessage(Messages.REST_NO_SUCH_FORECAST, request.getForecastId(), request.getJobId())
                    )
                );
            }
        } else {
            listener.onFailure(new ElasticsearchException("An error occurred while searching forecasts to delete", e));
        }
    }
}
