/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.dataframe;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.DataFrameAnalyticsInfoAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class RestDataFrameAnalyticsInfoAction extends BaseRestHandler {

    public RestDataFrameAnalyticsInfoAction(RestController controller) {
        controller.registerHandler(RestRequest.Method.GET, MachineLearning.BASE_PATH + "data_frame/analytics/_info", this);
        controller.registerHandler(RestRequest.Method.POST, MachineLearning.BASE_PATH + "data_frame/analytics/_info", this);
        controller.registerHandler(RestRequest.Method.GET, MachineLearning.BASE_PATH + "data_frame/analytics/{"
            + DataFrameAnalyticsConfig.ID.getPreferredName() + "}/_info", this);
        controller.registerHandler(RestRequest.Method.POST, MachineLearning.BASE_PATH + "data_frame/analytics/{"
            + DataFrameAnalyticsConfig.ID.getPreferredName() + "}/_info", this);
    }

    @Override
    public String getName() {
        return "ml_data_frame_analytics_info_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        final String jobId = restRequest.param(DataFrameAnalyticsConfig.ID.getPreferredName());

        if (Strings.isNullOrEmpty(jobId) && restRequest.hasContentOrSourceParam() == false) {
            throw ExceptionsHelper.badRequestException("Please provide a job [{}] or the config object",
                DataFrameAnalyticsConfig.ID.getPreferredName());
        }

        if (Strings.isNullOrEmpty(jobId) == false && restRequest.hasContentOrSourceParam()) {
            throw ExceptionsHelper.badRequestException("Please either provide a job [{}] or the config object but not both",
                DataFrameAnalyticsConfig.ID.getPreferredName());
        }

        // We need to consume the body before returning
        PutDataFrameAnalyticsAction.Request infoRequestFromBody = jobId == null ?
            PutDataFrameAnalyticsAction.Request.parseRequestForInfo(restRequest.contentOrSourceParamParser()) : null;

        return channel -> {
            RestToXContentListener<DataFrameAnalyticsInfoAction.Response> listener = new RestToXContentListener<>(channel);

            if (infoRequestFromBody != null) {
                client.execute(DataFrameAnalyticsInfoAction.INSTANCE, infoRequestFromBody, listener);
            } else {
                GetDataFrameAnalyticsAction.Request getRequest = new GetDataFrameAnalyticsAction.Request(jobId);
                getRequest.setAllowNoResources(false);
                client.execute(GetDataFrameAnalyticsAction.INSTANCE, getRequest, ActionListener.wrap(
                    getResponse -> {
                        List<DataFrameAnalyticsConfig> jobs = getResponse.getResources().results();
                        if (jobs.size() > 1) {
                            listener.onFailure(ExceptionsHelper.badRequestException("expected only one config but matched {}",
                                jobs.stream().map(DataFrameAnalyticsConfig::getId).collect(Collectors.toList())));
                        } else {
                            PutDataFrameAnalyticsAction.Request infoRequest = new PutDataFrameAnalyticsAction.Request(jobs.get(0));
                            client.execute(DataFrameAnalyticsInfoAction.INSTANCE, infoRequest, listener);
                        }
                    },
                    listener::onFailure
                ));
            }
        };
    }
}
