/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsStatsAction;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsStatsAction.Request;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsStatsAction.Response;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStateAndStats;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameInternalIndex;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.transforms.DataFrameTransformTask;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportGetDataFrameTransformsStatsAction extends
        TransportTasksAction<DataFrameTransformTask,
        GetDataFrameTransformsStatsAction.Request,
        GetDataFrameTransformsStatsAction.Response,
        GetDataFrameTransformsStatsAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetDataFrameTransformsStatsAction.class);

    private final Client client;
    private final DataFrameTransformsConfigManager dataFrameTransformsConfigManager;
    @Inject
    public TransportGetDataFrameTransformsStatsAction(TransportService transportService, ActionFilters actionFilters,
                                                      ClusterService clusterService, Client client,
                                                      DataFrameTransformsConfigManager dataFrameTransformsConfigManager) {
        super(GetDataFrameTransformsStatsAction.NAME, clusterService, transportService, actionFilters, Request::new, Response::new,
                Response::new, ThreadPool.Names.SAME);
        this.client = client;
        this.dataFrameTransformsConfigManager = dataFrameTransformsConfigManager;
    }

    @Override
    protected Response newResponse(Request request, List<Response> tasks, List<TaskOperationFailure> taskOperationFailures,
            List<FailedNodeException> failedNodeExceptions) {
        List<DataFrameTransformStateAndStats> responses = tasks.stream()
            .flatMap(r -> r.getTransformsStateAndStats().stream())
            .sorted(Comparator.comparing(DataFrameTransformStateAndStats::getId))
            .collect(Collectors.toList());
        return new Response(responses, taskOperationFailures, failedNodeExceptions);
    }

    @Override
    protected void taskOperation(Request request, DataFrameTransformTask task, ActionListener<Response> listener) {
        List<DataFrameTransformStateAndStats> transformsStateAndStats = Collections.emptyList();

        // Little extra insurance, make sure we only return transforms that aren't cancelled
        if (task.isCancelled() == false) {
            DataFrameTransformStateAndStats transformStateAndStats = new DataFrameTransformStateAndStats(task.getTransformId(),
                    task.getState(), task.getStats());
            transformsStateAndStats = Collections.singletonList(transformStateAndStats);
        }

        listener.onResponse(new Response(transformsStateAndStats));
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> finalListener) {
        final ClusterState state = clusterService.state();
        final DiscoveryNodes nodes = state.nodes();
        if (nodes.isLocalNodeElectedMaster()) {
            dataFrameTransformsConfigManager.expandTransformIds(request.getId(), request.getPageParams(), ActionListener.wrap(
                ids -> {
                    request.setExpandedIds(ids);
                    super.doExecute(task, request, ActionListener.wrap(
                        response -> collectStatsForTransformsWithoutTasks(request, response, finalListener),
                        finalListener::onFailure
                    ));
                },
                e -> {
                    // If the index to search, or the individual config is not there, just return empty
                    if (e instanceof ResourceNotFoundException) {
                        finalListener.onResponse(new Response(Collections.emptyList()));
                    } else {
                        finalListener.onFailure(e);
                    }
                }
            ));
        } else {
            // Delegates GetTransforms to elected master node, so it becomes the coordinating node.
            // Non-master nodes may have a stale cluster state that shows transforms which are cancelled
            // on the master, which makes testing difficult.
            if (nodes.getMasterNode() == null) {
                finalListener.onFailure(new MasterNotDiscoveredException("no known master nodes"));
            } else {
                transportService.sendRequest(nodes.getMasterNode(), actionName, request,
                        new ActionListenerResponseHandler<>(finalListener, Response::new));
            }
        }
    }

    private void collectStatsForTransformsWithoutTasks(Request request,
                                                       Response response,
                                                       ActionListener<Response> listener) {
        // We gathered all there is, no need to continue
        if (request.getExpandedIds().size() == response.getTransformsStateAndStats().size()) {
            listener.onResponse(response);
            return;
        }

        Set<String> transformsWithoutTasks = new HashSet<>(request.getExpandedIds());
        transformsWithoutTasks.removeAll(response.getTransformsStateAndStats().stream().map(DataFrameTransformStateAndStats::getId)
            .collect(Collectors.toList()));

        // Small assurance that we are at least below the max. Terms search has a hard limit of 10k, we should at least be below that.
        assert transformsWithoutTasks.size() <= Request.MAX_SIZE_RETURN;

        ActionListener<SearchResponse> searchStatsListener = ActionListener.wrap(
            searchResponse -> {
                List<ElasticsearchException> nodeFailures = new ArrayList<>(response.getNodeFailures());
                if (searchResponse.getShardFailures().length > 0) {
                    String msg = "transform statistics document search returned shard failures: " +
                        Arrays.toString(searchResponse.getShardFailures());
                    logger.error(msg);
                    nodeFailures.add(new ElasticsearchException(msg));
                }
                List<DataFrameTransformStateAndStats> allStateAndStats = response.getTransformsStateAndStats();
                for(SearchHit hit : searchResponse.getHits().getHits()) {
                    BytesReference source = hit.getSourceRef();
                    try {
                        DataFrameIndexerTransformStats stats = parseFromSource(source);
                        allStateAndStats.add(DataFrameTransformStateAndStats.initialStateAndStats(stats.getTransformId(), stats));
                        transformsWithoutTasks.remove(stats.getTransformId());
                    } catch (IOException e) {
                        listener.onFailure(new ElasticsearchParseException("Could not parse data frame transform stats", e));
                        return;
                    }
                }
                transformsWithoutTasks.forEach(transformId ->
                    allStateAndStats.add(DataFrameTransformStateAndStats.initialStateAndStats(transformId)));

                // Any transform in collection could NOT have a task, so, even though the list is initially sorted
                // it can easily become arbitrarily ordered based on which transforms don't have a task or stats docs
                allStateAndStats.sort(Comparator.comparing(DataFrameTransformStateAndStats::getId));

                listener.onResponse(new Response(allStateAndStats, response.getTaskFailures(), nodeFailures));
            },
            e -> {
                if (e instanceof IndexNotFoundException) {
                    listener.onResponse(response);
                } else {
                    listener.onFailure(e);
                }
            }
        );

        QueryBuilder builder = QueryBuilders.constantScoreQuery(QueryBuilders.boolQuery()
            .filter(QueryBuilders.termsQuery(DataFrameField.ID.getPreferredName(), transformsWithoutTasks))
            .filter(QueryBuilders.termQuery(DataFrameField.INDEX_DOC_TYPE.getPreferredName(), DataFrameIndexerTransformStats.NAME)));

        SearchRequest searchRequest = client.prepareSearch(DataFrameInternalIndex.INDEX_NAME)
            .addSort(DataFrameField.ID.getPreferredName(), SortOrder.ASC)
            .setQuery(builder)
            .request();

        ClientHelper.executeAsyncWithOrigin(client.threadPool().getThreadContext(),
            ClientHelper.DATA_FRAME_ORIGIN,
            searchRequest,
            searchStatsListener, client::search);
    }

    private static DataFrameIndexerTransformStats parseFromSource(BytesReference source) throws IOException {
        try (InputStream stream = source.streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                 .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
            return DataFrameIndexerTransformStats.fromXContent(parser);
        }
    }
}
