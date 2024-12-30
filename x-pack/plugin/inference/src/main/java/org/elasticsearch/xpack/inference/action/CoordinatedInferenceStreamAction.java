/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.UnifiedCompletionAction;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

/*
 * This coordinator maintains a stream for an inference response.
 * When called, an InferenceServiceResults is immediately returned on the client node with a stream Publisher.
 * The client subscribes to the publisher which opens the connection to the target node and begins the stream.
 * The connection remains open throughout the stream and is reused to send each chunk in the response stream.
 * The target node will push chunks to the client node through a separate Transport action request that targets the client node.
 * The client node will respond to those chunk requests with a request for more data or a stream cancellation.
 * When the stream is finished (or cancelled), the target node will call the listener and close the connection.
 *   +------------+       +-------------------------+                          +------------+           +-------------------------+
 *   | Client     |       | Client                  |                          | TargetNode |           | TargetNode              |
 *   | Subscriber |       | InferenceServiceResults |                          | Subscriber |           | InferenceServiceResults |
 *   +------------+       +-------------------------+                          +------------+           +-------------------------+
 *         |                           |                                             |                               |
 *         |--------[subscribe]------->|                                             |                               |
 *         |                           |  # This is a new Transport connection.      |                               |
 *         |                           |  # It stays open throughout the stream.     |                               |
 *         |                           |--[StreamRequest]--------------------------->|                               |
 *         |                           |                                             |--[subscribe]----------------->|
 *         |                           |                                             |                               |
 *         |                           |                                             |<---------------[onSubscribe]--|
 *         |                           |  # Each "response" is a new Transport       |                               |
 *         |                           |  # request that reuses the connection.      |                               |
 *         |                           |<----------------------[on_subscribe_chunk]--|                               |
 *         |<-----------[onSubscribe]--|                                             |                               |
 *         |                           |                                             |                               |
 *         |--[request 1 chunk]------->|                                             |                               |
 *         |                           |--[request 1 chunk]------------------------->|                               |
 *         |                           |                                             |--[request 1 chunk]----------->|
 *         |                           |                                             |                               |
 *         |                           |                                             |<--------------[result_chunk]--|
 *         |                           |<----------------------------[result_chunk]--|                               |
 *         |<----------[result_chunk]--|                                             |                               |
 *         .                           .                                             .                               .
 *         .                           .                                             .                               .
 *         .                           .                                             .                               .
 *         .                           .                                             .                               .
 *         |--[request 1 chunk]------->|                                             |                               |
 *         |                           |--[request 1 chunk]------------------------->|                               |
 *         |                           |                                             |--[request 1 chunk]----------->|
 *         |                           |                                             |                               |
 *         |                           |                                             |<----------------[onComplete]--|
 *         |                           | # This is the actual response that closes   |                               |
 *         |                           | # the overall connection.                   |                               |
 *         |                           |<---------------[ActionListener.onResponse]--|                               |
 *         |<------------[onComplete]--|                                             |                               |
 *         |                           |                                             |                               |
 *
 * Cancellations can happen at any time and will replace the "request 1 chunk" with a cancellation request that will cause the TargetNode
 * to complete the listener and clean up the task.
 */
public class CoordinatedInferenceStreamAction extends TransportAction<CoordinatedInferenceStreamAction.Request, InferenceAction.Response> {

    public static final String NAME = "cluster:internal/xpack/inference/node/streaming";
    public static final ActionType<InferenceAction.Response> INSTANCE = new ActionType<>(NAME);

    private static final Map<Long, ExistingStream> existingStreams = ConcurrentCollections.newConcurrentMap();
    private final TransportService transportService;
    private final NodeClient nodeClient;
    private final ThreadPool threadPool;

    @Inject
    public CoordinatedInferenceStreamAction(
        TransportService transportService,
        ActionFilters actionFilters,
        NodeClient nodeClient,
        ThreadPool threadPool
    ) {
        super(NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.transportService = transportService;
        this.nodeClient = nodeClient;
        this.threadPool = threadPool;

        new TransportInferenceStreamAction(transportService, actionFilters);
        new TransportStreamInferencePublisher(transportService, actionFilters);
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<InferenceAction.Response> listener) {
        // we don't need this class's special transport handling for non-streaming requests or for requests to the local node
        if (request.delegate.isStreaming() == false || request.targetNode.equals(transportService.getLocalNode())) {
            callInference(request.delegate, listener);
            return;
        }

        var streamId = task.getId();
        ActionListener.completeWith(listener, () -> new InferenceAction.Response(null, subscriber -> {
            var existingSubscriber = existingStreams.putIfAbsent(streamId, new ExistingStream(subscriber));
            if (existingSubscriber != null) {
                var exception = new IllegalStateException("already executing streaming inference task [" + streamId + "]");
                assert false : exception;
                subscriber.onError(exception);
            } else {
                transportService.sendRequest(
                    request.targetNode,
                    TransportInferenceStreamAction.ACTION_NAME,
                    new TransportInferenceStreamAction.StreamRequest(streamId, transportService.getLocalNode(), request.delegate),
                    TransportRequestOptions.timeout(request.timeout),
                    new ActionListenerResponseHandler<>(
                        // once the request is complete, we complete the subscriber
                        // if we get an error response, forward it to the subscriber
                        // in either case, always remove the subscriber from the map
                        ActionListener.runAfter(
                            ActionListener.wrap(r -> subscriber.onComplete(), subscriber::onError),
                            () -> existingStreams.remove(streamId)
                        ),
                        in -> ActionResponse.Empty.INSTANCE,
                        threadPool.executor(UTILITY_THREAD_POOL_NAME)
                    )
                );
            }
        }));
    }

    private void callInference(BaseInferenceActionRequest request, ActionListener<InferenceAction.Response> listener) {
        switch (request) {
            case UnifiedCompletionAction.Request req -> nodeClient.execute(UnifiedCompletionAction.INSTANCE, req, listener);
            case InferenceAction.Request req -> nodeClient.execute(InferenceAction.INSTANCE, req, listener);
            default -> throw new UnsupportedOperationException(
                "Unknown request type for inference coordination " + request.getClass().getName()
            );
        }
    }

    public static class Request extends ActionRequest {
        private final BaseInferenceActionRequest delegate;
        private final DiscoveryNode targetNode;
        private final TimeValue timeout;

        public Request(BaseInferenceActionRequest delegate, DiscoveryNode targetNode, @Nullable TimeValue timeout) {
            this.delegate = Objects.requireNonNull(delegate);
            this.targetNode = Objects.requireNonNull(targetNode);
            this.timeout = timeout;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }
    }

    /**
     * This runs on the TargetNode.
     * It will receive data from the InferenceAction stream and push it to the node that issued the request.
     * This acts as the Subscriber of the InferenceAction.Response.
     */
    private class TransportInferenceStreamAction extends HandledTransportAction<
        TransportInferenceStreamAction.StreamRequest,
        ActionResponse.Empty> {
        private static final String ACTION_NAME = NAME + "[stream]";

        protected TransportInferenceStreamAction(TransportService transportService, ActionFilters actionFilters) {
            super(ACTION_NAME, transportService, actionFilters, StreamRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        }

        private static class StreamRequest extends ActionRequest {
            private final long streamId;
            private final DiscoveryNode responseNode;
            private final BaseInferenceActionRequest delegate;

            private StreamRequest(long streamId, DiscoveryNode responseNode, BaseInferenceActionRequest delegate) {
                this.streamId = streamId;
                this.responseNode = responseNode;
                this.delegate = delegate;
            }

            private StreamRequest(StreamInput in) throws IOException {
                super(in);
                this.streamId = in.readLong();
                this.responseNode = new DiscoveryNode(in);
                this.delegate = in.readNamedWriteable(BaseInferenceActionRequest.class);
            }

            @Override
            public ActionRequestValidationException validate() {
                if (responseNode != null && delegate != null) {
                    return null;
                }
                var validationException = new ActionRequestValidationException();
                if (responseNode == null) {
                    validationException.addValidationError("There must be a [responseNode] to stream response chunks to.");
                }
                if (delegate == null) {
                    validationException.addValidationError("There must be a [delegate] request to call Inference with.");
                }
                return validationException;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeLong(streamId);
                responseNode.writeTo(out);
                out.writeNamedWriteable(delegate);
            }

            @Override
            public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
            }
        }

        @Override
        protected void doExecute(Task task, StreamRequest request, ActionListener<ActionResponse.Empty> listener) {
            var responseConnection = transportService.getConnection(request.responseNode);
            callInference(request.delegate, listener.delegateFailureAndWrap((l, r) -> {
                r.publisher().subscribe(new Flow.Subscriber<>() {
                    private volatile Flow.Subscription upstream;

                    @Override
                    public void onSubscribe(Flow.Subscription subscription) {
                        assert upstream == null : "Upstream must only be set once";
                        upstream = subscription;
                        sendResult(TransportStreamInferencePublisher.PublishRequest.ON_SUBSCRIBE_CHUNK);
                    }

                    @Override
                    public void onNext(InferenceServiceResults.Result item) {
                        sendResult(item);
                    }

                    private void sendResult(InferenceServiceResults.Result result) {
                        transportService.sendChildRequest(
                            responseConnection,
                            TransportStreamInferencePublisher.ACTION_NAME,
                            new TransportStreamInferencePublisher.PublishRequest(request.streamId, result),
                            task,
                            TransportRequestOptions.EMPTY,
                            new ActionListenerResponseHandler<>(listener.delegateFailureAndWrap((l, r) -> {
                                assert upstream != null : "Upstream must be set before we can send results.";
                                if (r.cancel) {
                                    upstream.cancel();
                                    // if we got cancelled, call the listener to close out responseConnection
                                    onComplete();
                                } else {
                                    // do not call listener, we'll do that later in onError or onComplete
                                    upstream.request(r.request);
                                }
                            }), TransportStreamInferencePublisher.PublishResponse::new, threadPool.executor(UTILITY_THREAD_POOL_NAME))
                        );
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        if (throwable instanceof Exception e) {
                            listener.onFailure(e);
                        } else {
                            ExceptionsHelper.maybeError(throwable).ifPresent(ExceptionsHelper::maybeDieOnAnotherThread);
                            listener.onFailure(new RuntimeException("Unhandled error while streaming"));
                        }
                    }

                    @Override
                    public void onComplete() {
                        l.onResponse(ActionResponse.Empty.INSTANCE);
                    }
                });
            }));
        }

    }

    /**
     * This runs on the original Coordinator node.
     * It will receive InferenceAction chunks and forward them to the original subscriber.
     * This acts as the Publisher for the InferenceAction.Response.
     */
    private static class TransportStreamInferencePublisher extends HandledTransportAction<
        TransportStreamInferencePublisher.PublishRequest,
        TransportStreamInferencePublisher.PublishResponse> {
        private static final String ACTION_NAME = NAME + "[publish]";

        protected TransportStreamInferencePublisher(TransportService transportService, ActionFilters actionFilters) {
            super(ACTION_NAME, transportService, actionFilters, PublishRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        }

        private static class PublishRequest extends ActionRequest {
            private static final InferenceServiceResults.Result ON_SUBSCRIBE_CHUNK = new InferenceServiceResults.Result() {
                private static final String NAME = "coordinated_inference_on_subscribe_chunk";

                @Override
                public String getWriteableName() {
                    return NAME;
                }

                @Override
                public void writeTo(StreamOutput out) {}

                @Override
                public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
                    return Collections.emptyIterator();
                }

                @Override
                public boolean equals(Object o) {
                    if (o == null || getClass() != o.getClass()) return false;
                    InferenceServiceResults.Result that = (InferenceServiceResults.Result) o;
                    return getWriteableName().equals(that.getWriteableName());
                }

                @Override
                public int hashCode() {
                    return Objects.hash(getWriteableName());
                }
            };

            private final long streamId;
            private final InferenceServiceResults.Result chunk;

            private PublishRequest(long streamId, InferenceServiceResults.Result chunk) {
                this.streamId = streamId;
                this.chunk = chunk;
            }

            private PublishRequest(StreamInput in) throws IOException {
                super(in);
                this.streamId = in.readLong();
                this.chunk = in.readNamedWriteable(InferenceServiceResults.Result.class);
            }

            @Override
            public ActionRequestValidationException validate() {
                if (chunk != null) {
                    return null;
                }
                var validationException = new ActionRequestValidationException();
                validationException.addValidationError("There must be a response [chunk] to publish the message.");
                return validationException;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeLong(streamId);
                out.writeNamedWriteable(chunk);
            }
        }

        private static class PublishResponse extends ActionResponse {
            private final long request;
            private final boolean cancel;

            private PublishResponse(long request, boolean cancel) {
                this.request = request;
                this.cancel = cancel;
            }

            private PublishResponse(StreamInput in) throws IOException {
                this.request = in.readLong();
                this.cancel = in.readBoolean();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeLong(request);
                out.writeBoolean(cancel);
            }
        }

        @Override
        protected void doExecute(Task task, PublishRequest request, ActionListener<PublishResponse> listener) {
            var existingStream = existingStreams.get(request.streamId);
            if (existingStream == null) {
                assert false : "Sent request to the wrong node, check task " + request.streamId;
                listener.onFailure(new IllegalStateException("Sent request to the wrong node, check task " + request.streamId));
            }

            if (PublishRequest.ON_SUBSCRIBE_CHUNK.equals(request.chunk)) {
                existingStream.onSubscribe(listener);
            } else {
                existingStream.onNext(request.chunk, listener);
            }
        }

    }

    /**
     * This runs on the original Coordinator node.
     * It wraps the Subscriber and provides backpressure and cancellation to the Publisher on the TargetNode via the ActionListener.
     */
    private static class ExistingStream {
        private static final PendingMessage EMPTY = new PendingMessage(false, null);

        // since cancellations can arrive asynchronously, we have to have a thread-safe way to report them back to the TargetNode.
        // PendingMessage will hold the listener for the normal execution and cancellations for the abnormal execution, and both need to be
        // updated together atomically.
        private final AtomicReference<PendingMessage> pendingMessages = new AtomicReference<>(EMPTY);

        private final Flow.Subscriber<? super InferenceServiceResults.Result> subscriber;
        private final AtomicBoolean onSubscribeCalled = new AtomicBoolean(false);

        private ExistingStream(Flow.Subscriber<? super InferenceServiceResults.Result> subscriber) {
            this.subscriber = subscriber;
        }

        public void onSubscribe(ActionListener<TransportStreamInferencePublisher.PublishResponse> listener) {
            if (onSubscribeCalled.compareAndSet(false, true)) {
                if (scheduleNextMessage(listener)) {
                    this.subscriber.onSubscribe(new ResponseSubscription());
                } else {
                    sendCancelMessage(listener);
                }
            } else {
                assert false : "onSubscribe should only be called once";
                listener.onFailure(new IllegalStateException("onSubscribe should only be called once"));
            }
        }

        private boolean scheduleNextMessage(ActionListener<TransportStreamInferencePublisher.PublishResponse> listener) {
            var pendingMessage = pendingMessages.updateAndGet(currentMessage -> {
                if (currentMessage.cancel) {
                    return currentMessage;
                } else {
                    return new PendingMessage(false, listener);
                }
            });
            return pendingMessage.cancel == false;
        }

        public void onNext(
            InferenceServiceResults.Result chunk,
            ActionListener<TransportStreamInferencePublisher.PublishResponse> listener
        ) {
            if (scheduleNextMessage(listener)) {
                subscriber.onNext(chunk);
            } else {
                sendCancelMessage(listener);
            }
        }

        private void sendCancelMessage(ActionListener<TransportStreamInferencePublisher.PublishResponse> listener) {
            listener.onResponse(new TransportStreamInferencePublisher.PublishResponse(0, true));
        }

        private record PendingMessage(boolean cancel, ActionListener<TransportStreamInferencePublisher.PublishResponse> listener) {}

        private class ResponseSubscription implements Flow.Subscription {
            @Override
            public void request(long n) {
                assert n == 1 : "We only support sending 1 request at a time";
                var pendingMessage = pendingMessages.getAndSet(EMPTY);
                if (pendingMessage.cancel) {
                    cancel();
                } else if (pendingMessage.listener == null) {
                    assert false : "Cannot request data more than once.";
                    subscriber.onError(new IllegalStateException("Cannot request data more than once."));
                } else {
                    pendingMessage.listener.onResponse(new TransportStreamInferencePublisher.PublishResponse(1, false));
                }
            }

            @Override
            public void cancel() {
                var pendingMessage = pendingMessages.getAndSet(new PendingMessage(true, null));
                if (pendingMessage.listener != null) {
                    sendCancelMessage(pendingMessage.listener);
                }
            }
        }
    }

    public static List<NamedWriteableRegistry.Entry> namedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(
                InferenceServiceResults.Result.class,
                TransportStreamInferencePublisher.PublishRequest.ON_SUBSCRIBE_CHUNK.getWriteableName(),
                in -> TransportStreamInferencePublisher.PublishRequest.ON_SUBSCRIBE_CHUNK
            )
        );
    }
}
