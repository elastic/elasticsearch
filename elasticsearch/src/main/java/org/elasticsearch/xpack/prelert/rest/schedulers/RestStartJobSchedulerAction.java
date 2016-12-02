/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.rest.schedulers;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.tasks.LoggingTaskListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.StartJobSchedulerAction;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.JobSchedulerStatus;
import org.elasticsearch.xpack.prelert.job.SchedulerState;
import org.elasticsearch.xpack.prelert.job.manager.JobManager;
import org.elasticsearch.xpack.prelert.job.messages.Messages;
import org.elasticsearch.xpack.prelert.job.metadata.Allocation;
import org.elasticsearch.xpack.prelert.job.scheduler.ScheduledJobRunner;

import java.io.IOException;

public class RestStartJobSchedulerAction extends BaseRestHandler {

    private static final String DEFAULT_START = "0";

    private final JobManager jobManager;
    private final ClusterService clusterService;

    @Inject
    public RestStartJobSchedulerAction(Settings settings, RestController controller, JobManager jobManager,
                                       ClusterService clusterService) {
        super(settings);
        this.jobManager = jobManager;
        this.clusterService = clusterService;
        controller.registerHandler(RestRequest.Method.POST,
                PrelertPlugin.BASE_PATH + "schedulers/{" + Job.ID.getPreferredName() + "}/_start", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());

        // This validation happens also in ScheduledJobRunner, the reason we do it here too is that if it fails there
        // we are unable to provide the user immediate feedback. We would create the task and the validation would fail
        // in the background, whereas now the validation failure is part of the response being returned.
        Job job = jobManager.getJobOrThrowIfUnknown(jobId);
        Allocation allocation = jobManager.getJobAllocation(jobId);
        ScheduledJobRunner.validate(job, allocation);

        StartJobSchedulerAction.Request jobSchedulerRequest;
        if (RestActions.hasBodyContent(restRequest)) {
            BytesReference bodyBytes = RestActions.getRestContent(restRequest);
            XContentParser parser = XContentFactory.xContent(bodyBytes).createParser(bodyBytes);
            jobSchedulerRequest = StartJobSchedulerAction.Request.parseRequest(jobId, parser, () -> parseFieldMatcher);
        } else {
            long startTimeMillis = parseDateOrThrow(restRequest.param(SchedulerState.START_TIME_MILLIS.getPreferredName(), DEFAULT_START),
                    SchedulerState.START_TIME_MILLIS.getPreferredName());
            Long endTimeMillis = null;
            if (restRequest.hasParam(SchedulerState.END_TIME_MILLIS.getPreferredName())) {
                endTimeMillis = parseDateOrThrow(restRequest.param(SchedulerState.END_TIME_MILLIS.getPreferredName()),
                        SchedulerState.END_TIME_MILLIS.getPreferredName());
            }
            SchedulerState schedulerState = new SchedulerState(JobSchedulerStatus.STARTED, startTimeMillis, endTimeMillis);
            jobSchedulerRequest = new StartJobSchedulerAction.Request(jobId, schedulerState);
        }
        return sendTask(client.executeLocally(StartJobSchedulerAction.INSTANCE, jobSchedulerRequest, LoggingTaskListener.instance()));
    }

    private RestChannelConsumer sendTask(Task task) throws IOException {
        return channel -> {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("task", clusterService.localNode().getId() + ":" + task.getId());
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
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
