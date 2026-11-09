/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.common.DelegatingProcessorTests.onError;
import static org.elasticsearch.xpack.inference.common.DelegatingProcessorTests.onNext;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class MistralUnifiedStreamingProcessorTests extends ESTestCase {

    private static MistralUnifiedStreamingProcessor processor() {
        return new MistralUnifiedStreamingProcessor((data, exception) -> exception != null ? exception : new RuntimeException(data));
    }

    private static Deque<ServerSentEvent> events(String... data) {
        var events = new ArrayDeque<ServerSentEvent>();
        for (var d : data) {
            events.add(new ServerSentEvent(d));
        }
        return events;
    }

    private static String chunk(String delta, String finishReason) {
        return "{\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"model\":\"mistral-reasoning-test\","
            + "\"choices\":[{\"index\":0,\"delta\":"
            + delta
            + ",\"finish_reason\":"
            + (finishReason == null ? "null" : "\"" + finishReason + "\"")
            + "}]}";
    }

    public void testStringContentStreamsUnchanged() throws Exception {
        var results = onNext(processor(), events(chunk("{\"role\":\"assistant\",\"content\":\"Hello\"}", null)));

        var message = results.chunks().getFirst().choices().getFirst().message();
        assertThat(message.content(), is("Hello"));
        assertNull(message.reasoning());
    }

    public void testListContentStreamsAsContentAndReasoning() throws Exception {
        var thinking = chunk("{\"content\":[{\"type\":\"thinking\",\"thinking\":[{\"type\":\"text\",\"text\":\"hmm\"}]}]}", null);
        var text = chunk("{\"content\":[{\"type\":\"text\",\"text\":\"Hi\"}]}", null);

        var results = onNext(processor(), events(thinking, text));

        assertThat(results.chunks().size(), is(2));
        var first = results.chunks().getFirst().choices().getFirst().message();
        assertThat(first.reasoning(), is("hmm"));
        assertThat(first.content(), is(""));
        var second = results.chunks().getLast().choices().getFirst().message();
        assertThat(second.content(), is("Hi"));
        assertNull(second.reasoning());
    }

    public void testFinishReasonErrorBecomesTypedError() {
        var throwable = onError(processor(), events(chunk("{\"content\":\"partial\"}", "error")));

        assertThat(throwable, instanceOf(UnifiedChatCompletionException.class));
        var exception = (UnifiedChatCompletionException) throwable;
        assertThat(exception.status(), is(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(exception.getMessage().contains("finish reason [error]"), is(true));
    }

    public void testFinishReasonStopIsNotAnError() throws Exception {
        var results = onNext(processor(), events(chunk("{\"content\":\"done\"}", "stop")));

        assertThat(results.chunks().getFirst().choices().getFirst().finishReason(), is("stop"));
    }

    public void testStreamEndingWithoutFinishReasonIsAnError() {
        var processor = processor();
        var error = new AtomicReference<Throwable>();
        var completed = new AtomicBoolean();
        subscribe(processor, error, completed);

        processor.onNext(events(chunk("{\"content\":\"partial\"}", null), "[DONE]"));
        processor.onComplete();

        assertThat(completed.get(), is(false));
        assertThat(error.get(), instanceOf(UnifiedChatCompletionException.class));
        assertThat(error.get().getMessage().contains("without a finish reason"), is(true));
    }

    public void testStreamEndingAfterFinishReasonCompletes() {
        var processor = processor();
        var error = new AtomicReference<Throwable>();
        var completed = new AtomicBoolean();
        subscribe(processor, error, completed);

        processor.onNext(events(chunk("{\"content\":\"done\"}", "stop"), "[DONE]"));
        processor.onComplete();

        assertThat(completed.get(), is(true));
        assertNull(error.get());
    }

    private static void subscribe(MistralUnifiedStreamingProcessor processor, AtomicReference<Throwable> error, AtomicBoolean completed) {
        processor.onSubscribe(mock(Flow.Subscription.class));
        processor.subscribe(new Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {}

            @Override
            public void onNext(StreamingUnifiedChatCompletionResults.Results item) {}

            @Override
            public void onError(Throwable throwable) {
                error.set(throwable);
            }

            @Override
            public void onComplete() {
                completed.set(true);
            }
        });
    }
}
