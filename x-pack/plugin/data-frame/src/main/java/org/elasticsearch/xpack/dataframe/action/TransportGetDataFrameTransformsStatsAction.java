/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsStatsAction;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsStatsAction.Request;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsStatsAction.Response;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStateAndStats;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameInternalIndex;
import org.elasticsearch.xpack.dataframe.persistence.DataFramePersistentTaskUtils;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.transforms.DataFrameTransformTask;
import org.elasticsearch.xpack.dataframe.util.BatchedDataIterator;

import java.io.IOException;
import java.io.InputStream;
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

        assert task.getTransformId().equals(request.getId()) || request.getId().equals(MetaData.ALL);

        // Little extra insurance, make sure we only return transforms that aren't cancelled
        if (task.isCancelled() == false) {
            DataFrameTransformStateAndStats transformStateAndStats = new DataFrameTransformStateAndStats(task.getTransformId(),
                    task.getState(), task.getStats());
            transformsStateAndStats = Collections.singletonList(transformStateAndStats);
        }

        listener.onResponse(new Response(transformsStateAndStats));
    }

    @Override
    // TODO gather stats from docs when moved out of allocated task
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final ClusterState state = clusterService.state();
        final DiscoveryNodes nodes = state.nodes();

        if (nodes.isLocalNodeElectedMaster()) {
            if (DataFramePersistentTaskUtils.stateHasDataFrameTransforms(request.getId(), state)) {
                ActionListener<Response> transformStatsListener = ActionListener.wrap(
                    response -> collectStatsForTransformsWithoutTasks(request, response, listener),
                    listener::onFailure
                );
                super.doExecute(task, request, transformStatsListener);
            } else {
                // If we don't have any tasks, pass empty collection to this method
                collectStatsForTransformsWithoutTasks(request, new Response(Collections.emptyList()), listener);
            }

        } else {
            // Delegates GetTransforms to elected master node, so it becomes the coordinating node.
            // Non-master nodes may have a stale cluster state that shows transforms which are cancelled
            // on the master, which makes testing difficult.
            if (nodes.getMasterNode() == null) {
                listener.onFailure(new MasterNotDiscoveredException("no known master nodes"));
            } else {
                transportService.sendRequest(nodes.getMasterNode(), actionName, request,
                        new ActionListenerResponseHandler<>(listener, Response::new));
            }
        }
    }

    // TODO correct when we start storing stats in docs, right now, just return STOPPED and empty stats
    private void collectStatsForTransformsWithoutTasks(Request request,
                                                       Response response,
                                                       ActionListener<Response> listener) {
        if (request.getId().equals(MetaData.ALL) == false) {
            // If we did not find any tasks && this is NOT for ALL, verify that the single config exists, and return as stopped
            // Empty other wise
            if (response.getTransformsStateAndStats().isEmpty()) {
                dataFrameTransformsConfigManager.getTransformConfiguration(request.getId(), ActionListener.wrap(
                    config ->
                        listener.onResponse(
                            new Response(Collections.singletonList(DataFrameTransformStateAndStats.initialStateAndStats(config.getId())))),
                    exception -> {
                        if (exception instanceof ResourceNotFoundException) {
                            listener.onResponse(new Response(Collections.emptyList()));
                        } else {
                            listener.onFailure(exception);
                        }
                    }
                ));
            } else {
                // If it was not ALL && we DO have stored stats, simply return those as we found them all, since we only support 1 or all
                listener.onResponse(response);
            }
            return;
        }
        // We only do this mass collection if we are getting ALL tasks
        TransformIdCollector collector = new TransformIdCollector();
        collector.execute(ActionListener.wrap(
            allIds -> {
                response.getTransformsStateAndStats().forEach(
                    tsas -> allIds.remove(tsas.getId())
                );
                List<DataFrameTransformStateAndStats> statsWithoutTasks = allIds.stream()
                    .map(DataFrameTransformStateAndStats::initialStateAndStats)
                    .collect(Collectors.toList());
                statsWithoutTasks.addAll(response.getTransformsStateAndStats());
                statsWithoutTasks.sort(Comparator.comparing(DataFrameTransformStateAndStats::getId));
                listener.onResponse(new Response(statsWithoutTasks));
            },
            listener::onFailure
        ));
    }

    /**
     * This class recursively queries a scroll search over all transform_ids and puts them in a set
     */
    private class TransformIdCollector extends BatchedDataIterator<String, Set<String>> {

        private final Set<String> ids = new HashSet<>();
        TransformIdCollector() {
            super(client, DataFrameInternalIndex.INDEX_NAME);
        }

        void execute(final ActionListener<Set<String>> finalListener) {
            if (this.hasNext()) {
                next(ActionListener.wrap(
                    setOfIds -> execute(finalListener),
                    finalListener::onFailure
                ));
            } else {
                finalListener.onResponse(ids);
            }
        }

        @Override
        protected QueryBuilder getQuery() {
            return QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(DataFrameField.INDEX_DOC_TYPE.getPreferredName(), DataFrameTransformConfig.NAME));
        }

        @Override
        protected String map(SearchHit hit) {
            BytesReference source = hit.getSourceRef();
            try (InputStream stream = source.streamInput();
                 XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(NamedXContentRegistry.EMPTY,
                     LoggingDeprecationHandler.INSTANCE, stream)) {
                return (String)parser.map().get(DataFrameField.ID.getPreferredName());
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parse bucket", e);
            }
        }

        @Override
        protected Set<String> getCollection() {
            return ids;
        }

        @Override
        protected SortOrder sortOrder() {
            return SortOrder.ASC;
        }

        @Override
        protected String sortField() {
            return DataFrameField.ID.getPreferredName();
        }

        @Override
        protected FetchSourceContext getFetchSourceContext() {
            return new FetchSourceContext(true, new String[]{DataFrameField.ID.getPreferredName()}, new String[]{});
        }
    }


}
