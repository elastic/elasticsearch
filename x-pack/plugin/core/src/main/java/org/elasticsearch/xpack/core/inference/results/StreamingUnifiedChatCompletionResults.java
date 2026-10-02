/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.inference.DequeUtils;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionChunkResponse;

import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.Flow;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.core.inference.DequeUtils.dequeEquals;
import static org.elasticsearch.xpack.core.inference.DequeUtils.dequeHashCode;
import static org.elasticsearch.xpack.core.inference.DequeUtils.readDeque;

/**
 * Chat Completion results that only contain a Flow.Publisher.
 */
public record StreamingUnifiedChatCompletionResults(Flow.Publisher<Results> publisher) implements InferenceServiceResults {

    /**
     * OpenAI Spec only returns one result at a time, and Chat Completion adheres to that spec as much as possible.
     * So we will insert a buffer in between the upstream data and the downstream client so that we only send one request at a time.
     */
    public StreamingUnifiedChatCompletionResults(Flow.Publisher<Results> publisher) {
        Deque<ChatCompletionChunkResponse> buffer = new LinkedBlockingDeque<>();
        AtomicBoolean onComplete = new AtomicBoolean();
        this.publisher = downstream -> {
            publisher.subscribe(new Flow.Subscriber<>() {
                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    downstream.onSubscribe(new Flow.Subscription() {
                        @Override
                        public void request(long n) {
                            var nextItem = buffer.poll();
                            if (nextItem != null) {
                                downstream.onNext(new Results(DequeUtils.of(nextItem)));
                            } else if (onComplete.get()) {
                                downstream.onComplete();
                            } else {
                                subscription.request(n);
                            }
                        }

                        @Override
                        public void cancel() {
                            subscription.cancel();
                        }
                    });
                }

                @Override
                public void onNext(Results item) {
                    var chunks = item.chunks();
                    var firstItem = chunks.poll();
                    chunks.forEach(buffer::offer);
                    downstream.onNext(new Results(DequeUtils.of(firstItem)));
                }

                @Override
                public void onError(Throwable throwable) {
                    downstream.onError(throwable);
                }

                @Override
                public void onComplete() {
                    // only complete if the buffer is empty, so that the client has a chance to drain the buffer
                    if (onComplete.compareAndSet(false, true)) {
                        if (buffer.isEmpty()) {
                            downstream.onComplete();
                        }
                    }
                }
            });
        };
    }

    @Override
    public boolean isStreaming() {
        return true;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        throw new UnsupportedOperationException("Not implemented");
    }

    public record Results(Deque<ChatCompletionChunkResponse> chunks) implements InferenceServiceResults.Result {
        public static final String NAME = "streaming_unified_chat_completion_results";

        public Results(StreamInput in) throws IOException {
            this(readDeque(in, ChatCompletionChunkResponse::new));
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.flatMap(chunks.iterator(), c -> c.toStreamingXContentChunked(params));
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(chunks, StreamOutput::writeWriteable);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Results results = (Results) o;
            return dequeEquals(chunks, results.chunks());
        }

        @Override
        public int hashCode() {
            return dequeHashCode(chunks);
        }
    }
}
