/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.dataframe;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.PreviewDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestPreviewDataFrameAnalyticsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, BASE_PATH + "data_frame/analytics/_preview"),
            new Route(POST, BASE_PATH + "data_frame/analytics/_preview"),
            new Route(GET, BASE_PATH + "data_frame/analytics/{" + DataFrameAnalyticsConfig.ID + "}/_preview"),
            new Route(POST, BASE_PATH + "data_frame/analytics/{" + DataFrameAnalyticsConfig.ID + "}/_preview")
        );
    }

    @Override
    public String getName() {
        return "ml_preview_data_frame_analytics_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        final String jobId = restRequest.param(DataFrameAnalyticsConfig.ID.getPreferredName());

        if (Strings.isNullOrEmpty(jobId) && restRequest.hasContentOrSourceParam() == false) {
            throw ExceptionsHelper.badRequestException(
                "Please provide a job [{}] or the config object",
                DataFrameAnalyticsConfig.ID.getPreferredName()
            );
        }

        if (Strings.isNullOrEmpty(jobId) == false && restRequest.hasContentOrSourceParam()) {
            throw ExceptionsHelper.badRequestException(
                "Please provide either a job [{}] or the config object but not both",
                DataFrameAnalyticsConfig.ID.getPreferredName()
            );
        }
        final PreviewDataFrameAnalyticsAction.Request.Builder requestBuilder = Strings.isNullOrEmpty(jobId)
            ? PreviewDataFrameAnalyticsAction.Request.fromXContent(restRequest.contentOrSourceParamParser())
            : new PreviewDataFrameAnalyticsAction.Request.Builder();

        return channel -> {
            RestToXContentListener<PreviewDataFrameAnalyticsAction.Response> listener = new RestToXContentListener<>(channel);
            RestCancellableNodeClient cancellableClient = new RestCancellableNodeClient(client, restRequest.getHttpChannel());

            if (requestBuilder.getConfig() != null) {
                cancellableClient.execute(PreviewDataFrameAnalyticsAction.INSTANCE, requestBuilder.build(), listener);
            } else {
                GetDataFrameAnalyticsAction.Request getRequest = new GetDataFrameAnalyticsAction.Request(jobId);
                getRequest.setAllowNoResources(false);
                cancellableClient.execute(GetDataFrameAnalyticsAction.INSTANCE, getRequest, ActionListener.wrap(getResponse -> {
                    List<DataFrameAnalyticsConfig> jobs = getResponse.getResources().results();
                    if (jobs.size() > 1) {
                        listener.onFailure(
                            ExceptionsHelper.badRequestException(
                                "expected only one config but matched {}",
                                jobs.stream().map(DataFrameAnalyticsConfig::getId).collect(Collectors.toList())
                            )
                        );
                    } else {
                        cancellableClient.execute(
                            PreviewDataFrameAnalyticsAction.INSTANCE,
                            requestBuilder.setConfig(jobs.get(0)).build(),
                            listener
                        );
                    }
                }, listener::onFailure));
            }
        };
    }
}
