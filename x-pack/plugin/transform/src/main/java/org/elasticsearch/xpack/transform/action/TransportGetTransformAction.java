/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.action.AbstractTransportGetResourcesAction;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction.Request;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction.Response;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.transforms.TransformNodes;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import java.util.Set;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.xpack.core.transform.TransformField.INDEX_DOC_TYPE;

public class TransportGetTransformAction extends AbstractTransportGetResourcesAction<TransformConfig, Request, Response> {

    private static final String DANGLING_TASK_ERROR_MESSAGE_FORMAT =
        "Found task for transform [%s], but no configuration for it. To delete this transform use DELETE with force=true.";

    private final ClusterService clusterService;
    private final TransformConfigManager transformConfigManager;

    @Inject
    public TransportGetTransformAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Client client,
        NamedXContentRegistry xContentRegistry,
        TransformServices transformServices
    ) {
        super(GetTransformAction.NAME, transportService, actionFilters, Request::new, client, xContentRegistry);
        this.clusterService = clusterService;
        this.transformConfigManager = transformServices.configManager();
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        final ClusterState clusterState = clusterService.state();
        TransformNodes.warnIfNoTransformNodes(clusterState);

        // Step 2: Search for all the transform tasks (matching the request) that *do not* have corresponding transform config.
        ActionListener<QueryPage<TransformConfig>> searchTransformConfigsListener = listener.delegateFailureAndWrap((l, r) -> {
            getAllTransformIds(r, TimeValue.THIRTY_SECONDS, l.delegateFailureAndWrap((ll, transformConfigIds) -> {
                var errors = TransformTask.findTransformTasks(request.getId(), clusterState)
                    .stream()
                    .map(PersistentTasksCustomMetadata.PersistentTask::getId)
                    .filter(not(transformConfigIds::contains))
                    .map(
                        transformId -> new Response.Error("dangling_task", Strings.format(DANGLING_TASK_ERROR_MESSAGE_FORMAT, transformId))
                    )
                    .toList();
                ll.onResponse(new Response(r.results(), r.count(), errors.isEmpty() ? null : errors));
            }));
        });

        // Step 1: Search for all the transform configs matching the request.
        searchResources(request, parentTaskId, searchTransformConfigsListener);
    }

    @Override
    protected ParseField getResultsField() {
        return TransformField.TRANSFORMS;
    }

    @Override
    protected String[] getIndices() {
        return new String[] {
            TransformInternalIndexConstants.INDEX_NAME_PATTERN,
            TransformInternalIndexConstants.INDEX_NAME_PATTERN_DEPRECATED };
    }

    @Override
    protected TransformConfig parse(XContentParser parser) {
        return TransformConfig.fromXContent(parser, null, true);
    }

    @Override
    protected ResourceNotFoundException notFoundException(String resourceId) {
        return new ResourceNotFoundException(TransformMessages.getMessage(TransformMessages.REST_UNKNOWN_TRANSFORM, resourceId));
    }

    @Override
    protected String executionOrigin() {
        return ClientHelper.TRANSFORM_ORIGIN;
    }

    @Override
    protected String extractIdFromResource(TransformConfig transformConfig) {
        return transformConfig.getId();
    }

    @Override
    protected QueryBuilder additionalQuery() {
        return QueryBuilders.termQuery(INDEX_DOC_TYPE.getPreferredName(), TransformConfig.NAME);
    }

    @Override
    protected SearchSourceBuilder customSearchOptions(SearchSourceBuilder searchSourceBuilder) {
        // sort by Transform's id in ASC order, matching what we will do above for the active TransformTasks
        return searchSourceBuilder.sort("_index", SortOrder.DESC).sort(TransformField.ID.getPreferredName(), SortOrder.ASC);
    }

    private void getAllTransformIds(QueryPage<TransformConfig> queryPage, TimeValue timeout, ActionListener<Set<String>> listener) {
        if (queryPage.count() == queryPage.results().size()) {
            listener.onResponse(queryPage.results().stream().map(TransformConfig::getId).collect(toSet()));
        } else {
            // if we do not have all of our transform ids already, we have to go get them
            transformConfigManager.getAllTransformIds(timeout, listener);
        }
    }

}
