/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.xpack.persistent.PersistentTasksService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class StopDatafeedAction
        extends Action<StopDatafeedAction.Request, StopDatafeedAction.Response, StopDatafeedAction.RequestBuilder> {

    public static final StopDatafeedAction INSTANCE = new StopDatafeedAction();
    public static final String NAME = "cluster:admin/xpack/ml/datafeed/stop";
    public static final ParseField TIMEOUT = new ParseField("timeout");
    public static final ParseField FORCE = new ParseField("force");
    public static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueMinutes(5);

    private StopDatafeedAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends BaseTasksRequest<Request> implements ToXContent {

        public static ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, datafeedId) -> request.datafeedId = datafeedId, DatafeedConfig.ID);
            PARSER.declareString((request, val) ->
                    request.setStopTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
            PARSER.declareBoolean(Request::setForce, FORCE);
        }

        public static Request fromXContent(XContentParser parser) {
            return parseRequest(null, parser);
        }

        public static Request parseRequest(String datafeedId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (datafeedId != null) {
                request.datafeedId = datafeedId;
            }
            return request;
        }

        private String datafeedId;
        private String[] resolvedDatafeedIds;
        private TimeValue stopTimeout = DEFAULT_TIMEOUT;
        private boolean force = false;

        public Request(String datafeedId) {
            this.datafeedId = ExceptionsHelper.requireNonNull(datafeedId, DatafeedConfig.ID.getPreferredName());
            this.resolvedDatafeedIds = new String[] { datafeedId };
        }

        Request() {
        }

        private String getDatafeedId() {
            return datafeedId;
        }

        private String[] getResolvedDatafeedIds() {
            return resolvedDatafeedIds;
        }

        private void setResolvedDatafeedIds(String[] resolvedDatafeedIds) {
            this.resolvedDatafeedIds = resolvedDatafeedIds;
        }

        public TimeValue getStopTimeout() {
            return stopTimeout;
        }

        public void setStopTimeout(TimeValue stopTimeout) {
            this.stopTimeout = ExceptionsHelper.requireNonNull(stopTimeout, TIMEOUT.getPreferredName());
        }

        public boolean isForce() {
            return force;
        }

        public void setForce(boolean force) {
            this.force = force;
        }

        @Override
        public boolean match(Task task) {
            for (String id : resolvedDatafeedIds) {
                String expectedDescription = "datafeed-" + id;
                if (task instanceof StartDatafeedAction.DatafeedTask && expectedDescription.equals(task.getDescription())){
                    return true;
                }
            }
            return false;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            datafeedId = in.readString();
            resolvedDatafeedIds = in.readStringArray();
            stopTimeout = new TimeValue(in);
            force = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(datafeedId);
            out.writeStringArray(resolvedDatafeedIds);
            stopTimeout.writeTo(out);
            out.writeBoolean(force);
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeedId, stopTimeout, force);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(DatafeedConfig.ID.getPreferredName(), datafeedId);
            builder.field(TIMEOUT.getPreferredName(), stopTimeout.getStringRep());
            builder.field(FORCE.getPreferredName(), force);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(datafeedId, other.datafeedId) &&
                    Objects.equals(stopTimeout, other.stopTimeout) &&
                    Objects.equals(force, other.force);
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable {

        private boolean stopped;

        public Response(boolean stopped) {
            super(null, null);
            this.stopped = stopped;
        }

        public Response(StreamInput in) throws IOException {
            readFrom(in);
        }

        public Response() {
        }

        public boolean isStopped() {
            return stopped;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            stopped = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(stopped);
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client, StopDatafeedAction action) {
            super(client, action, new Request());
        }
    }

    public static class TransportAction extends TransportTasksAction<StartDatafeedAction.DatafeedTask, Request, Response, Response> {

        private final PersistentTasksService persistentTasksService;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               ClusterService clusterService, PersistentTasksService persistentTasksService) {
            super(settings, StopDatafeedAction.NAME, threadPool, clusterService, transportService, actionFilters,
                    indexNameExpressionResolver, Request::new, Response::new, ThreadPool.Names.MANAGEMENT);
            this.persistentTasksService = persistentTasksService;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            ClusterState state = clusterService.state();
            MlMetadata mlMetadata = state.getMetaData().custom(MlMetadata.TYPE);
            PersistentTasksCustomMetaData tasks = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

            List<String> resolvedDatafeeds = resolve(request.getDatafeedId(), mlMetadata, tasks);
            if (resolvedDatafeeds.isEmpty()) {
                listener.onResponse(new Response(true));
                return;
            }
            request.setResolvedDatafeedIds(resolvedDatafeeds.toArray(new String[resolvedDatafeeds.size()]));

            if (request.force) {
                final AtomicInteger counter = new AtomicInteger();
                final AtomicArray<Exception> failures = new AtomicArray<>(resolvedDatafeeds.size());

                for (String datafeedId : resolvedDatafeeds) {
                    PersistentTask<?> datafeedTask = MlMetadata.getDatafeedTask(datafeedId, tasks);
                    if (datafeedTask != null) {
                        persistentTasksService.cancelPersistentTask(datafeedTask.getId(), new ActionListener<PersistentTask<?>>() {
                            @Override
                            public void onResponse(PersistentTask<?> persistentTask) {
                                if (counter.incrementAndGet() == resolvedDatafeeds.size()) {
                                    sendResponseOrFailure(request.getDatafeedId(), listener, failures);
                                }
                            }
                            @Override
                            public void onFailure(Exception e) {
                                final int slot = counter.incrementAndGet();
                                failures.set(slot - 1, e);
                                if (slot == resolvedDatafeeds.size()) {
                                    sendResponseOrFailure(request.getDatafeedId(), listener, failures);
                                }
                            }
                        });
                    } else {
                        String msg = "Requested datafeed [" + request.getDatafeedId() + "] be force-stopped, but " +
                                "datafeed's task could not be found.";
                        logger.warn(msg);
                        final int slot = counter.incrementAndGet();
                        failures.set(slot - 1, new RuntimeException(msg));
                        if (slot == resolvedDatafeeds.size()) {
                            sendResponseOrFailure(request.getDatafeedId(), listener, failures);
                        }
                    }
                }
            } else {
                Set<String> executorNodes = new HashSet<>();
                Map<String, String> datafeedIdToPersistentTaskId = new HashMap<>();

                for (String datafeedId : resolvedDatafeeds) {
                    PersistentTask<?> datafeedTask = validateAndReturnDatafeedTask(datafeedId, mlMetadata, tasks);
                    executorNodes.add(datafeedTask.getExecutorNode());
                    datafeedIdToPersistentTaskId.put(datafeedId, datafeedTask.getId());
                }

                ActionListener<Response> finalListener =
                        ActionListener.wrap(r -> waitForDatafeedStopped(datafeedIdToPersistentTaskId, request, r, listener), listener::onFailure);

                request.setNodes(executorNodes.toArray(new String[executorNodes.size()]));
                super.doExecute(task, request, finalListener);
            }
        }

        private void sendResponseOrFailure(String datafeedId, ActionListener<Response> listener,
                AtomicArray<Exception> failures) {
            List<Exception> catchedExceptions = failures.asList();
            if (catchedExceptions.size() == 0) {
                listener.onResponse(new Response(true));
                return;
            }

            String msg = "Failed to stop datafeed [" + datafeedId + "] with [" + catchedExceptions.size()
                + "] failures, rethrowing last, all Exceptions: ["
                + catchedExceptions.stream().map(Exception::getMessage).collect(Collectors.joining(", "))
                + "]";

            ElasticsearchException e = new ElasticsearchException(msg,
                    catchedExceptions.get(0));
            listener.onFailure(e);
        }

        // Wait for datafeed to be marked as stopped in cluster state, which means the datafeed persistent task has been removed
        // This api returns when task has been cancelled, but that doesn't mean the persistent task has been removed from cluster state,
        // so wait for that to happen here.
        void waitForDatafeedStopped(Map<String, String> datafeedIdToPersistentTaskId, Request request, Response response,
                                    ActionListener<Response> listener) {
            persistentTasksService.waitForPersistentTasksStatus(persistentTasksCustomMetaData -> {
                for (Map.Entry<String, String> entry : datafeedIdToPersistentTaskId.entrySet()) {
                    String persistentTaskId = entry.getValue();
                    if (persistentTasksCustomMetaData.getTask(persistentTaskId) != null) {
                        return false;
                    }
                }
                return true;
            }, request.getTimeout(), new ActionListener<Boolean>() {
                @Override
                public void onResponse(Boolean result) {
                    listener.onResponse(response);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }

        @Override
        protected Response newResponse(Request request, List<Response> tasks, List<TaskOperationFailure> taskOperationFailures,
                                       List<FailedNodeException> failedNodeExceptions) {
            // number of resolved data feeds should be equal to the number of
            // tasks, otherwise something went wrong
            if (request.getResolvedDatafeedIds().length != tasks.size()) {
                if (taskOperationFailures.isEmpty() == false) {
                    throw org.elasticsearch.ExceptionsHelper
                            .convertToElastic(taskOperationFailures.get(0).getCause());
                } else if (failedNodeExceptions.isEmpty() == false) {
                    throw org.elasticsearch.ExceptionsHelper
                            .convertToElastic(failedNodeExceptions.get(0));
                } else {
                    throw new IllegalStateException(
                            "Expected [" + request.getResolvedDatafeedIds().length
                                    + "] number of tasks but " + "got [" + tasks.size() + "]");
                }
            }

            return new Response(tasks.stream().allMatch(Response::isStopped));
        }

        @Override
        protected Response readTaskResponse(StreamInput in) throws IOException {
            return new Response(in);
        }

        @Override
        protected void taskOperation(Request request, StartDatafeedAction.DatafeedTask task, ActionListener<Response> listener) {
            task.stop("stop_datafeed (api)", request.getStopTimeout());
            listener.onResponse(new Response(true));
        }

        @Override
        protected boolean accumulateExceptions() {
            return true;
        }
    }

    static List<String> resolve(String datafeedId, MlMetadata mlMetadata,
            PersistentTasksCustomMetaData tasks) {
        if (!Job.ALL.equals(datafeedId)) {
            return Collections.singletonList(datafeedId);
        }

        if (mlMetadata.getDatafeeds().isEmpty()) {
            return Collections.emptyList();
        }

        List<String> matched_datafeeds = new ArrayList<>();

        for (Map.Entry<String, DatafeedConfig> datafeedEntry : mlMetadata.getDatafeeds()
                .entrySet()) {
            String resolvedDatafeedId = datafeedEntry.getKey();
            DatafeedConfig datafeed = datafeedEntry.getValue();

            DatafeedState datafeedState = MlMetadata.getDatafeedState(datafeed.get().getId(),
                    tasks);

            if (datafeedState == DatafeedState.STOPPED) {
                continue;
            }

            matched_datafeeds.add(resolvedDatafeedId);
        }

        return matched_datafeeds;
    }

    static PersistentTask<?> validateAndReturnDatafeedTask(String datafeedId, MlMetadata mlMetadata, PersistentTasksCustomMetaData tasks) {
        DatafeedConfig datafeed = mlMetadata.getDatafeed(datafeedId);
        if (datafeed == null) {
            throw new ResourceNotFoundException(Messages.getMessage(Messages.DATAFEED_NOT_FOUND, datafeedId));
        }
        PersistentTask<?> task = MlMetadata.getDatafeedTask(datafeedId, tasks);
        if (task == null) {
            throw ExceptionsHelper.conflictStatusException("Cannot stop datafeed [" + datafeedId +
                    "] because it has already been stopped");
        }
        return task;
    }
}
