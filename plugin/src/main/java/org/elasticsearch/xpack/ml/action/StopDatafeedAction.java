/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
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
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.xpack.persistent.PersistentTasksService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StopDatafeedAction
        extends Action<StopDatafeedAction.Request, StopDatafeedAction.Response, StopDatafeedAction.RequestBuilder> {

    public static final StopDatafeedAction INSTANCE = new StopDatafeedAction();
    public static final String NAME = "cluster:admin/xpack/ml/datafeed/stop";
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

    public static class Request extends BaseTasksRequest<Request> implements ToXContentObject {

        public static final ParseField TIMEOUT = new ParseField("timeout");
        public static final ParseField FORCE = new ParseField("force");
        public static final ParseField ALLOW_NO_DATAFEEDS = new ParseField("allow_no_datafeeds");

        public static ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, datafeedId) -> request.datafeedId = datafeedId, DatafeedConfig.ID);
            PARSER.declareString((request, val) ->
                    request.setStopTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
            PARSER.declareBoolean(Request::setForce, FORCE);
            PARSER.declareBoolean(Request::setAllowNoDatafeeds, ALLOW_NO_DATAFEEDS);
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
        private String[] resolvedStartedDatafeedIds;
        private TimeValue stopTimeout = DEFAULT_TIMEOUT;
        private boolean force = false;
        private boolean allowNoDatafeeds = true;

        public Request(String datafeedId) {
            this.datafeedId = ExceptionsHelper.requireNonNull(datafeedId, DatafeedConfig.ID.getPreferredName());
            this.resolvedStartedDatafeedIds = new String[] { datafeedId };
        }

        Request() {
        }

        private String getDatafeedId() {
            return datafeedId;
        }

        private String[] getResolvedStartedDatafeedIds() {
            return resolvedStartedDatafeedIds;
        }

        private void setResolvedStartedDatafeedIds(String[] resolvedStartedDatafeedIds) {
            this.resolvedStartedDatafeedIds = resolvedStartedDatafeedIds;
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

        public boolean allowNoDatafeeds() {
            return allowNoDatafeeds;
        }

        public void setAllowNoDatafeeds(boolean allowNoDatafeeds) {
            this.allowNoDatafeeds = allowNoDatafeeds;
        }

        @Override
        public boolean match(Task task) {
            for (String id : resolvedStartedDatafeedIds) {
                String expectedDescription = MlMetadata.datafeedTaskId(id);
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
            resolvedStartedDatafeedIds = in.readStringArray();
            stopTimeout = new TimeValue(in);
            force = in.readBoolean();
            if (in.getVersion().onOrAfter(Version.V_6_1_0)) {
                allowNoDatafeeds = in.readBoolean();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(datafeedId);
            out.writeStringArray(resolvedStartedDatafeedIds);
            stopTimeout.writeTo(out);
            out.writeBoolean(force);
            if (out.getVersion().onOrAfter(Version.V_6_1_0)) {
                out.writeBoolean(allowNoDatafeeds);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeedId, stopTimeout, force, allowNoDatafeeds);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(DatafeedConfig.ID.getPreferredName(), datafeedId);
            builder.field(TIMEOUT.getPreferredName(), stopTimeout.getStringRep());
            builder.field(FORCE.getPreferredName(), force);
            builder.field(ALLOW_NO_DATAFEEDS.getPreferredName(), allowNoDatafeeds);
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
                    Objects.equals(force, other.force) &&
                    Objects.equals(allowNoDatafeeds, other.allowNoDatafeeds);
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable {

        private boolean stopped;

        public Response(boolean stopped) {
            super(null, null);
            this.stopped = stopped;
        }

        public Response(StreamInput in) throws IOException {
            super(null, null);
            readFrom(in);
        }

        public Response() {
            super(null, null);
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
                    indexNameExpressionResolver, Request::new, Response::new, MachineLearning.UTILITY_THREAD_POOL_NAME);
            this.persistentTasksService = persistentTasksService;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            final ClusterState state = clusterService.state();
            final DiscoveryNodes nodes = state.nodes();
            if (nodes.isLocalNodeElectedMaster() == false) {
                // Delegates stop datafeed to elected master node, so it becomes the coordinating node.
                // See comment in StartDatafeedAction.Transport class for more information.
                if (nodes.getMasterNode() == null) {
                    listener.onFailure(new MasterNotDiscoveredException("no known master node"));
                } else {
                    transportService.sendRequest(nodes.getMasterNode(), actionName, request,
                            new ActionListenerResponseHandler<>(listener, Response::new));
                }
            } else {
                MlMetadata mlMetadata = state.getMetaData().custom(MlMetadata.TYPE);
                PersistentTasksCustomMetaData tasks = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

                List<String> startedDatafeeds = new ArrayList<>();
                List<String> stoppingDatafeeds = new ArrayList<>();
                resolveDataFeedIds(request, mlMetadata, tasks, startedDatafeeds, stoppingDatafeeds);
                if (startedDatafeeds.isEmpty() && stoppingDatafeeds.isEmpty()) {
                    listener.onResponse(new Response(true));
                    return;
                }
                request.setResolvedStartedDatafeedIds(startedDatafeeds.toArray(new String[startedDatafeeds.size()]));

                if (request.force) {
                    forceStopDatafeed(request, listener, tasks, startedDatafeeds);
                } else {
                    normalStopDatafeed(task, request, listener, tasks, startedDatafeeds, stoppingDatafeeds);
                }
            }
        }

        private void normalStopDatafeed(Task task, Request request, ActionListener<Response> listener,
                                        PersistentTasksCustomMetaData tasks,
                                        List<String> startedDatafeeds, List<String> stoppingDatafeeds) {
            Set<String> executorNodes = new HashSet<>();
            for (String datafeedId : startedDatafeeds) {
                PersistentTask<?> datafeedTask = MlMetadata.getDatafeedTask(datafeedId, tasks);
                if (datafeedTask == null || datafeedTask.isAssigned() == false) {
                    String message = "Cannot stop datafeed [" + datafeedId + "] because the datafeed does not have an assigned node." +
                            " Use force stop to stop the datafeed";
                    listener.onFailure(ExceptionsHelper.conflictStatusException(message));
                    return;
                } else {
                    executorNodes.add(datafeedTask.getExecutorNode());
                }
            }

            request.setNodes(executorNodes.toArray(new String[executorNodes.size()]));

            // wait for started and stopping datafeeds
            // Map datafeedId -> datafeed task Id.
            List<String> allDataFeedsToWaitFor = Stream.concat(
                    startedDatafeeds.stream().map(id -> MlMetadata.datafeedTaskId(id)),
                    stoppingDatafeeds.stream().map(id -> MlMetadata.datafeedTaskId(id)))
                    .collect(Collectors.toList());

            ActionListener<Response> finalListener = ActionListener.wrap(
                    r -> waitForDatafeedStopped(allDataFeedsToWaitFor, request, r, listener),
                    listener::onFailure);

            super.doExecute(task, request, finalListener);
        }

        private void forceStopDatafeed(final Request request, final ActionListener<Response> listener,
                                       PersistentTasksCustomMetaData tasks, final List<String> startedDatafeeds) {
            final AtomicInteger counter = new AtomicInteger();
            final AtomicArray<Exception> failures = new AtomicArray<>(startedDatafeeds.size());

            for (String datafeedId : startedDatafeeds) {
                PersistentTask<?> datafeedTask = MlMetadata.getDatafeedTask(datafeedId, tasks);
                if (datafeedTask != null) {
                    persistentTasksService.cancelPersistentTask(datafeedTask.getId(), new ActionListener<PersistentTask<?>>() {
                        @Override
                        public void onResponse(PersistentTask<?> persistentTask) {
                            if (counter.incrementAndGet() == startedDatafeeds.size()) {
                                sendResponseOrFailure(request.getDatafeedId(), listener, failures);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            final int slot = counter.incrementAndGet();
                            failures.set(slot - 1, e);
                            if (slot == startedDatafeeds.size()) {
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
                    if (slot == startedDatafeeds.size()) {
                        sendResponseOrFailure(request.getDatafeedId(), listener, failures);
                    }
                }
            }
        }

        @Override
        protected void taskOperation(Request request, StartDatafeedAction.DatafeedTask datafeedTaskTask,
                                     ActionListener<Response> listener) {
            DatafeedState taskStatus = DatafeedState.STOPPING;
            datafeedTaskTask.updatePersistentStatus(taskStatus, ActionListener.wrap(task -> {
                        // we need to fork because we are now on a network threadpool
                        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(new AbstractRunnable() {
                            @Override
                            public void onFailure(Exception e) {
                                listener.onFailure(e);
                            }

                            @Override
                            protected void doRun() throws Exception {
                                datafeedTaskTask.stop("stop_datafeed (api)", request.getStopTimeout());
                                listener.onResponse(new Response(true));
                            }
                        });
                    },
                    e -> {
                        if (e instanceof ResourceNotFoundException) {
                            // the task has disappeared so must have stopped
                            listener.onResponse(new Response(true));
                        } else {
                            listener.onFailure(e);
                        }
                    }
            ));
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
        void waitForDatafeedStopped(List<String> datafeedPersistentTaskIds, Request request, Response response,
                                    ActionListener<Response> listener) {
            persistentTasksService.waitForPersistentTasksStatus(persistentTasksCustomMetaData -> {
                for (String persistentTaskId: datafeedPersistentTaskIds) {
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
            if (request.getResolvedStartedDatafeedIds().length != tasks.size()) {
                if (taskOperationFailures.isEmpty() == false) {
                    throw org.elasticsearch.ExceptionsHelper
                            .convertToElastic(taskOperationFailures.get(0).getCause());
                } else if (failedNodeExceptions.isEmpty() == false) {
                    throw org.elasticsearch.ExceptionsHelper
                            .convertToElastic(failedNodeExceptions.get(0));
                } else {
                    // This can happen we the actual task in the node no longer exists,
                    // which means the datafeed(s) have already been closed.
                    return new Response(true);
                }
            }

            return new Response(tasks.stream().allMatch(Response::isStopped));
        }

        @Override
        protected Response readTaskResponse(StreamInput in) throws IOException {
            return new Response(in);
        }

    }

    /**
     * Resolve the requested datafeeds and add their IDs to one of the list
     * arguments depending on datafeed state.
     *
     * @param request The stop datafeed request
     * @param mlMetadata ML Metadata
     * @param tasks Persistent task meta data
     * @param startedDatafeedIds Started datafeed ids are added to this list
     * @param stoppingDatafeedIds Stopping datafeed ids are added to this list
     */
    static void resolveDataFeedIds(Request request, MlMetadata mlMetadata,
                                   PersistentTasksCustomMetaData tasks,
                                   List<String> startedDatafeedIds,
                                   List<String> stoppingDatafeedIds) {

        Set<String> expandedDatafeedIds = mlMetadata.expandDatafeedIds(request.getDatafeedId(), request.allowNoDatafeeds());
        for (String expandedDatafeedId : expandedDatafeedIds) {
            validateDatafeedTask(expandedDatafeedId, mlMetadata);
            addDatafeedTaskIdAccordingToState(expandedDatafeedId, MlMetadata.getDatafeedState(expandedDatafeedId, tasks),
                    startedDatafeedIds, stoppingDatafeedIds);
        }
    }

    private static void addDatafeedTaskIdAccordingToState(String datafeedId,
                                                      DatafeedState datafeedState,
                                                      List<String> startedDatafeedIds,
                                                      List<String> stoppingDatafeedIds) {
        switch (datafeedState) {
            case STARTED:
                startedDatafeedIds.add(datafeedId);
                break;
            case STOPPED:
                break;
            case STOPPING:
                stoppingDatafeedIds.add(datafeedId);
                break;
            default:
                break;
        }
    }
    /**
     * Validate the stop request.
     * Throws an {@code ResourceNotFoundException} if there is no datafeed
     * with id {@code datafeedId}
     * @param datafeedId The datafeed Id
     * @param mlMetadata ML meta data
     */
    static void validateDatafeedTask(String datafeedId, MlMetadata mlMetadata) {
        DatafeedConfig datafeed = mlMetadata.getDatafeed(datafeedId);
        if (datafeed == null) {
            throw new ResourceNotFoundException(Messages.getMessage(Messages.DATAFEED_NOT_FOUND, datafeedId));
        }
    }
}
