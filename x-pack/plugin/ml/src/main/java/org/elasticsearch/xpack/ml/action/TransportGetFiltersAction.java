/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.action.AbstractTransportGetResourcesAction;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.action.GetFiltersAction;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;

import java.io.IOException;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class TransportGetFiltersAction extends AbstractTransportGetResourcesAction<
    MlFilter,
    GetFiltersAction.Request,
    GetFiltersAction.Response> {

    private final ClusterService clusterService;

    @Inject
    public TransportGetFiltersAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry
    ) {
        super(GetFiltersAction.NAME, transportService, actionFilters, GetFiltersAction.Request::new, client, xContentRegistry);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, GetFiltersAction.Request request, ActionListener<GetFiltersAction.Response> listener) {
        request.setAllowNoResources(true);
        searchResources(
            request,
            new TaskId(clusterService.localNode().getId(), task.getId()),
            listener.delegateFailureAndWrap((l, filters) -> l.onResponse(new GetFiltersAction.Response(filters)))
        );
    }

    @Override
    protected ParseField getResultsField() {
        return MlFilter.RESULTS_FIELD;
    }

    @Override
    protected String[] getIndices() {
        return new String[] { MlMetaIndex.indexName() };
    }

    @Override
    protected MlFilter parse(XContentParser parser) throws IOException {
        return MlFilter.LENIENT_PARSER.parse(parser, null).build();
    }

    @Override
    protected ResourceNotFoundException notFoundException(String resourceId) {
        return new ResourceNotFoundException("Unable to find filter [" + resourceId + "]");
    }

    @Override
    protected String executionOrigin() {
        return ML_ORIGIN;
    }

    @Override
    protected String extractIdFromResource(MlFilter mlFilter) {
        return mlFilter.getId();
    }

    @Override
    protected QueryBuilder additionalQuery() {
        return QueryBuilders.termQuery(MlFilter.TYPE.getPreferredName(), MlFilter.FILTER_TYPE);
    }
}
