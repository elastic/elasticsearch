/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.action.AbstractTransportGetResourcesAction;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class TransportGetDataFrameAnalyticsAction extends AbstractTransportGetResourcesAction<
    DataFrameAnalyticsConfig,
    GetDataFrameAnalyticsAction.Request,
    GetDataFrameAnalyticsAction.Response> {

    @Inject
    public TransportGetDataFrameAnalyticsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(
            GetDataFrameAnalyticsAction.NAME,
            transportService,
            actionFilters,
            GetDataFrameAnalyticsAction.Request::new,
            client,
            xContentRegistry
        );
    }

    @Override
    protected ParseField getResultsField() {
        return GetDataFrameAnalyticsAction.Response.RESULTS_FIELD;
    }

    @Override
    protected String[] getIndices() {
        return new String[] { MlConfigIndex.indexName() };
    }

    @Override
    protected DataFrameAnalyticsConfig parse(XContentParser parser) {
        return DataFrameAnalyticsConfig.LENIENT_PARSER.apply(parser, null).build();
    }

    @Override
    protected ResourceNotFoundException notFoundException(String resourceId) {
        return ExceptionsHelper.missingDataFrameAnalytics(resourceId);
    }

    @Override
    protected void doExecute(
        Task task,
        GetDataFrameAnalyticsAction.Request request,
        ActionListener<GetDataFrameAnalyticsAction.Response> listener
    ) {
        searchResources(
            request,
            ActionListener.wrap(queryPage -> listener.onResponse(new GetDataFrameAnalyticsAction.Response(queryPage)), listener::onFailure)
        );
    }

    @Nullable
    protected QueryBuilder additionalQuery() {
        return QueryBuilders.termQuery(DataFrameAnalyticsConfig.CONFIG_TYPE.getPreferredName(), DataFrameAnalyticsConfig.TYPE);
    }

    @Override
    protected String executionOrigin() {
        return ML_ORIGIN;
    }

    @Override
    protected String extractIdFromResource(DataFrameAnalyticsConfig config) {
        return config.getId();
    }
}
