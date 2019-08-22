/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

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
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsAction;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsAction.Request;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsAction.Response;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameInternalIndex;

import static org.elasticsearch.xpack.core.dataframe.DataFrameField.INDEX_DOC_TYPE;


public class TransportGetDataFrameTransformsAction extends AbstractTransportGetResourcesAction<DataFrameTransformConfig,
                                                                                               Request,
                                                                                               Response> {

    @Inject
    public TransportGetDataFrameTransformsAction(TransportService transportService, ActionFilters actionFilters,
                                                 Client client, NamedXContentRegistry xContentRegistry) {
        super(GetDataFrameTransformsAction.NAME, transportService, actionFilters, Request::new, client, xContentRegistry);
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
        return DataFrameField.TRANSFORMS;
    }

    @Override
    protected String[] getIndices() {
        return new String[]{DataFrameInternalIndex.INDEX_NAME_PATTERN};
    }

    @Override
    protected DataFrameTransformConfig parse(XContentParser parser) {
        return DataFrameTransformConfig.fromXContent(parser, null, true);
    }

    @Override
    protected ResourceNotFoundException notFoundException(String resourceId) {
        return new ResourceNotFoundException(
            DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_TRANSFORM, resourceId));
    }

    @Override
    protected String executionOrigin() {
        return ClientHelper.DATA_FRAME_ORIGIN;
    }

    @Override
    protected String extractIdFromResource(DataFrameTransformConfig transformConfig) {
        return transformConfig.getId();
    }

    @Override
    protected QueryBuilder additionalQuery() {
        return QueryBuilders.termQuery(INDEX_DOC_TYPE.getPreferredName(), DataFrameTransformConfig.NAME);
    }

    @Override
    protected SearchSourceBuilder customSearchOptions(SearchSourceBuilder searchSourceBuilder) {
        return searchSourceBuilder.sort("_index", SortOrder.DESC);
    }

}
