/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.DeprecationHandler;
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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;


public class TransportDeleteForecastAction extends HandledTransportAction<DeleteForecastAction.Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDeleteForecastAction.class);

    private final Client client;
    private final ClusterService clusterService;
    private static final int MAX_FORECAST_TO_SEARCH = 10_000;

    private static final Set<ForecastRequestStatus> DELETABLE_STATUSES =
        EnumSet.of(ForecastRequestStatus.FINISHED, ForecastRequestStatus.FAILED);

    @Inject
    public TransportDeleteForecastAction(TransportService transportService,
                                         ActionFilters actionFilters,
                                         Client client,
                                         ClusterService clusterService) {
        super(DeleteForecastAction.NAME, transportService, actionFilters, DeleteForecastAction.Request::new);
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, DeleteForecastAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        final String jobId = request.getJobId();

        String forecastsExpression = request.getForecastId();
        final String[] forecastIds = Strings.tokenizeToStringArray(forecastsExpression, ",");
        ActionListener<SearchResponse> forecastStatsHandler = ActionListener.wrap(
            searchResponse -> deleteForecasts(searchResponse, request, listener),
            e -> listener.onFailure(new ElasticsearchException("An error occurred while searching forecasts to delete", e)));

        SearchSourceBuilder source = new SearchSourceBuilder();

        BoolQueryBuilder builder = QueryBuilders.boolQuery();
        BoolQueryBuilder innerBool = QueryBuilders.boolQuery().must(
            QueryBuilders.termQuery(Result.RESULT_TYPE.getPreferredName(), ForecastRequestStats.RESULT_TYPE_VALUE));
        if (Strings.isAllOrWildcard(forecastIds) == false) {
            innerBool.must(QueryBuilders.termsQuery(Forecast.FORECAST_ID.getPreferredName(), new HashSet<>(Arrays.asList(forecastIds))));
        }

        source.query(builder.filter(innerBool));

        SearchRequest searchRequest = new SearchRequest(AnomalyDetectorsIndex.jobResultsAliasedName(jobId));
        searchRequest.source(source);

        executeAsyncWithOrigin(client, ML_ORIGIN, SearchAction.INSTANCE, searchRequest, forecastStatsHandler);
    }

    static void validateForecastState(Collection<ForecastRequestStats> forecastsToDelete, JobState jobState, String jobId) {
        List<String> badStatusForecasts = forecastsToDelete.stream()
            .filter((f) -> DELETABLE_STATUSES.contains(f.getStatus()) == false)
            .map(ForecastRequestStats::getForecastId)
            .collect(Collectors.toList());
        if (badStatusForecasts.size() > 0 && JobState.OPENED.equals(jobState)) {
            throw ExceptionsHelper.conflictStatusException(
                Messages.getMessage(Messages.REST_CANNOT_DELETE_FORECAST_IN_CURRENT_STATE, badStatusForecasts, jobId));
        }
    }

    private void deleteForecasts(SearchResponse searchResponse,
                                 DeleteForecastAction.Request request,
                                 ActionListener<AcknowledgedResponse> listener) {
        final String jobId = request.getJobId();
        Set<ForecastRequestStats> forecastsToDelete;
        try {
            forecastsToDelete = parseForecastsFromSearch(searchResponse);
        } catch (IOException e) {
            listener.onFailure(e);
            return;
        }

        if (forecastsToDelete.isEmpty()) {
            if (Strings.isAllOrWildcard(new String[]{request.getForecastId()}) &&
                request.isAllowNoForecasts()) {
                listener.onResponse(new AcknowledgedResponse(true));
            } else {
                listener.onFailure(
                    new ResourceNotFoundException(Messages.getMessage(Messages.REST_NO_SUCH_FORECAST, request.getForecastId(), jobId)));
            }
            return;
        }
        final ClusterState state = clusterService.state();
        PersistentTasksCustomMetadata persistentTasks = state.metadata().custom(PersistentTasksCustomMetadata.TYPE);
        JobState jobState = MlTasks.getJobState(jobId, persistentTasks);
        try {
            validateForecastState(forecastsToDelete, jobState, jobId);
        } catch (ElasticsearchException ex) {
            listener.onFailure(ex);
            return;
        }

        final List<String> forecastIds = forecastsToDelete.stream().map(ForecastRequestStats::getForecastId).collect(Collectors.toList());
        DeleteByQueryRequest deleteByQueryRequest = buildDeleteByQuery(jobId, forecastIds);

        executeAsyncWithOrigin(client, ML_ORIGIN, DeleteByQueryAction.INSTANCE, deleteByQueryRequest, ActionListener.wrap(
            response -> {
                if (response.isTimedOut()) {
                    listener.onFailure(
                        new TimeoutException("Delete request timed out. Successfully deleted " +
                            response.getDeleted() + " forecast documents from job [" + jobId + "]"));
                    return;
                }
                if ((response.getBulkFailures().isEmpty() && response.getSearchFailures().isEmpty()) == false) {
                    Tuple<RestStatus, Throwable> statusAndReason = getStatusAndReason(response);
                    listener.onFailure(
                        new ElasticsearchStatusException(statusAndReason.v2().getMessage(), statusAndReason.v1(), statusAndReason.v2()));
                    return;
                }
                logger.info("Deleted forecast(s) [{}] from job [{}]", forecastIds, jobId);
                listener.onResponse(new AcknowledgedResponse(true));
            },
            listener::onFailure));
    }

    private static Tuple<RestStatus, Throwable> getStatusAndReason(final BulkByScrollResponse response) {
        RestStatus status = RestStatus.OK;
        Throwable reason = new Exception("Unknown error");
        //Getting the max RestStatus is sort of arbitrary, would the user care about 5xx over 4xx?
        //Unsure of a better way to return an appropriate and possibly actionable cause to the user.
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

    private static Set<ForecastRequestStats> parseForecastsFromSearch(SearchResponse searchResponse) throws IOException {
        SearchHits hits = searchResponse.getHits();
        List<ForecastRequestStats> allStats = new ArrayList<>(hits.getHits().length);
        for (SearchHit hit : hits) {
            try (InputStream stream = hit.getSourceRef().streamInput();
                 XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(
                     NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, stream)) {
                allStats.add(ForecastRequestStats.STRICT_PARSER.apply(parser, null));
            }
        }
        return new HashSet<>(allStats);
    }

    private DeleteByQueryRequest buildDeleteByQuery(String jobId, List<String> forecastsToDelete) {
        DeleteByQueryRequest request = new DeleteByQueryRequest()
            .setAbortOnVersionConflict(false) //since these documents are not updated, a conflict just means it was deleted previously
            .setMaxDocs(MAX_FORECAST_TO_SEARCH)
            .setSlices(AbstractBulkByScrollRequest.AUTO_SLICES);

        request.indices(AnomalyDetectorsIndex.jobResultsAliasedName(jobId));
        BoolQueryBuilder innerBoolQuery = QueryBuilders.boolQuery();
        innerBoolQuery
            .must(QueryBuilders.termsQuery(Result.RESULT_TYPE.getPreferredName(),
                ForecastRequestStats.RESULT_TYPE_VALUE, Forecast.RESULT_TYPE_VALUE))
            .must(QueryBuilders.termsQuery(Forecast.FORECAST_ID.getPreferredName(),
                forecastsToDelete));

        QueryBuilder query = QueryBuilders.boolQuery().filter(innerBoolQuery);
        request.setQuery(query);
        request.setRefresh(true);
        return request;
    }
}
