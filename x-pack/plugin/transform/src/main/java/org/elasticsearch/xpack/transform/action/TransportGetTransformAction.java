/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.action.AbstractTransportGetResourcesAction;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction.Request;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction.Response;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;

import static org.elasticsearch.xpack.core.transform.TransformField.INDEX_DOC_TYPE;


public class TransportGetTransformAction extends AbstractTransportGetResourcesAction<TransformConfig,
                                                                                     Request,
                                                                                     Response> {

    @Inject
    public TransportGetTransformAction(TransportService transportService, ActionFilters actionFilters, Client client,
                                       NamedXContentRegistry xContentRegistry) {
        this(GetTransformAction.NAME, transportService, actionFilters, client, xContentRegistry);
    }

    protected TransportGetTransformAction(String name, TransportService transportService, ActionFilters actionFilters, Client client,
                                          NamedXContentRegistry xContentRegistry) {
        super(name, transportService, actionFilters, Request::new, client, xContentRegistry);
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        searchResources(request, ActionListener.wrap(
            r -> listener.onResponse(new Response(r.results(), r.count())),
            listener::onFailure
        ));
    }

    @Override
    protected ParseField getResultsField() {
        return TransformField.TRANSFORMS;
    }

    @Override
    protected String[] getIndices() {
        return new String[] { TransformInternalIndexConstants.INDEX_NAME_PATTERN,
            TransformInternalIndexConstants.INDEX_NAME_PATTERN_DEPRECATED };
    }

    @Override
    protected TransformConfig parse(XContentParser parser) {
        return TransformConfig.fromXContent(parser, null, true);
    }

    @Override
    protected ResourceNotFoundException notFoundException(String resourceId) {
        return new ResourceNotFoundException(
            TransformMessages.getMessage(TransformMessages.REST_UNKNOWN_TRANSFORM, resourceId));
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
        return searchSourceBuilder.sort("_index", SortOrder.DESC);
    }

}
