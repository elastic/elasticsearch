/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.schedulers;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.LoggingTaskListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.MlPlugin;
import org.elasticsearch.xpack.ml.action.StartSchedulerAction;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.ml.scheduler.ScheduledJobRunner;
import org.elasticsearch.xpack.ml.scheduler.SchedulerConfig;
import org.elasticsearch.xpack.ml.scheduler.SchedulerStatus;
import org.elasticsearch.xpack.ml.utils.SchedulerStatusObserver;

import java.io.IOException;

public class RestStartSchedulerAction extends BaseRestHandler {

    private static final String DEFAULT_START = "0";

    private final ClusterService clusterService;
    private final SchedulerStatusObserver schedulerStatusObserver;

    @Inject
    public RestStartSchedulerAction(Settings settings, RestController controller, ThreadPool threadPool,
                                    ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;
        this.schedulerStatusObserver = new SchedulerStatusObserver(threadPool, clusterService);
        controller.registerHandler(RestRequest.Method.POST,
                MlPlugin.BASE_PATH + "schedulers/{" + SchedulerConfig.ID.getPreferredName() + "}/_start", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String schedulerId = restRequest.param(SchedulerConfig.ID.getPreferredName());

        // This validation happens also in ScheduledJobRunner, the reason we do it here too is that if it fails there
        // we are unable to provide the user immediate feedback. We would create the task and the validation would fail
        // in the background, whereas now the validation failure is part of the response being returned.
        MlMetadata mlMetadata = clusterService.state().metaData().custom(MlMetadata.TYPE);
        ScheduledJobRunner.validate(schedulerId, mlMetadata);

        StartSchedulerAction.Request jobSchedulerRequest;
        if (restRequest.hasContentOrSourceParam()) {
            XContentParser parser = restRequest.contentOrSourceParamParser();
            jobSchedulerRequest = StartSchedulerAction.Request.parseRequest(schedulerId, parser);
        } else {
            long startTimeMillis = parseDateOrThrow(restRequest.param(StartSchedulerAction.START_TIME.getPreferredName(),
                    DEFAULT_START), StartSchedulerAction.START_TIME.getPreferredName());
            Long endTimeMillis = null;
            if (restRequest.hasParam(StartSchedulerAction.END_TIME.getPreferredName())) {
                endTimeMillis = parseDateOrThrow(restRequest.param(StartSchedulerAction.END_TIME.getPreferredName()),
                        StartSchedulerAction.END_TIME.getPreferredName());
            }
            jobSchedulerRequest = new StartSchedulerAction.Request(schedulerId, startTimeMillis);
            jobSchedulerRequest.setEndTime(endTimeMillis);
        }
        TimeValue startTimeout = restRequest.paramAsTime("start_timeout", TimeValue.timeValueSeconds(30));
        return channel -> {
            Task task = client.executeLocally(StartSchedulerAction.INSTANCE, jobSchedulerRequest, LoggingTaskListener.instance());
            schedulerStatusObserver.waitForStatus(schedulerId, startTimeout, SchedulerStatus.STARTED, e -> {
                if (e != null) {
                    try {
                        channel.sendResponse(new BytesRestResponse(channel, e));
                    } catch (IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                } else {
                    try (XContentBuilder builder = channel.newBuilder()) {
                        builder.startObject();
                        builder.field("task", clusterService.localNode().getId() + ":" + task.getId());
                        builder.endObject();
                        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                    } catch (IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                }

            });
        };
    }

    static long parseDateOrThrow(String date, String paramName) {
        try {
            return DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parser().parseMillis(date);
        } catch (IllegalArgumentException e) {
            String msg = Messages.getMessage(Messages.REST_INVALID_DATETIME_PARAMS, paramName, date);
            throw new ElasticsearchParseException(msg, e);
        }
    }
}
