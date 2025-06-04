/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.Flow;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.inference.common.DelegatingProcessorTests.onError;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CohereStreamingProcessorTests extends ESTestCase {

    public void testParseErrorCallsOnError() {
        var item = new ArrayDeque<String>();
        item.offer("this is not json");

        var exception = onError(new CohereStreamingProcessor(), item);
        assertThat(exception, instanceOf(XContentParseException.class));
    }

    public void testUnrecognizedEventCallsOnError() {
        var item = new ArrayDeque<String>();
        item.offer("{\"event_type\":\"test\"}");

        var exception = onError(new CohereStreamingProcessor(), item);
        assertThat(exception, instanceOf(IOException.class));
        assertThat(exception.getMessage(), equalTo("Unknown eventType found: test"));
    }

    public void testMissingTextCallsOnError() {
        var item = new ArrayDeque<String>();
        item.offer("{\"event_type\":\"text-generation\"}");

        var exception = onError(new CohereStreamingProcessor(), item);
        assertThat(exception, instanceOf(IOException.class));
        assertThat(exception.getMessage(), equalTo("Null text found in text-generation cohere event"));
    }

    public void testEmptyResultsRequestsMoreData() throws Exception {
        var emptyDeque = new ArrayDeque<String>();

        var processor = new CohereStreamingProcessor();

        Flow.Subscriber<ChunkedToXContent> downstream = mock();
        processor.subscribe(downstream);

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);

        processor.next(emptyDeque);

        verify(upstream, times(1)).request(1);
        verify(downstream, times(0)).onNext(any());
    }

    public void testNonDataEventsAreSkipped() throws Exception {
        var item = new ArrayDeque<String>();
        item.offer("{\"event_type\":\"stream-start\"}");
        item.offer("{\"event_type\":\"search-queries-generation\"}");
        item.offer("{\"event_type\":\"search-results\"}");
        item.offer("{\"event_type\":\"citation-generation\"}");
        item.offer("{\"event_type\":\"tool-calls-generation\"}");
        item.offer("{\"event_type\":\"tool-calls-chunk\"}");

        var processor = new CohereStreamingProcessor();

        Flow.Subscriber<ChunkedToXContent> downstream = mock();
        processor.subscribe(downstream);

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);

        processor.next(item);

        verify(upstream, times(1)).request(1);
        verify(downstream, times(0)).onNext(any());
    }

    public void testParseError() {
        var json = "{\"event_type\":\"stream-end\", \"finish_reason\":\"ERROR\", \"response\":{ \"text\": \"a wild error appears\" }}";
        testError(json, e -> {
            assertThat(e.status().getStatus(), equalTo(500));
            assertThat(e.getMessage(), containsString("a wild error appears"));
        });
    }

    private void testError(String json, Consumer<ElasticsearchStatusException> test) {
        var item = new ArrayDeque<String>();
        item.offer(json);

        var processor = new CohereStreamingProcessor();

        Flow.Subscriber<ChunkedToXContent> downstream = mock();
        processor.subscribe(downstream);

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);

        try {
            processor.next(item);
            fail("Expected an exception to be thrown");
        } catch (ElasticsearchStatusException e) {
            test.accept(e);
        } catch (Exception e) {
            fail(e, "Expected an exception of type ElasticsearchStatusException to be thrown");
        }
    }

    public void testParseToxic() {
        var json = "{\"event_type\":\"stream-end\", \"finish_reason\":\"ERROR_TOXIC\", \"response\":{ \"text\": \"by britney spears\" }}";
        testError(json, e -> {
            assertThat(e.status().getStatus(), equalTo(500));
            assertThat(e.getMessage(), containsString("by britney spears"));
        });
    }

    public void testParseLimit() {
        var json = "{\"event_type\":\"stream-end\", \"finish_reason\":\"ERROR_LIMIT\", \"response\":{ \"text\": \"over the limit\" }}";
        testError(json, e -> {
            assertThat(e.status().getStatus(), equalTo(429));
            assertThat(e.getMessage(), containsString("over the limit"));
        });
    }

    public void testNonErrorFinishesAreSkipped() throws Exception {
        var item = new ArrayDeque<String>();
        item.offer("{\"event_type\":\"stream-end\", \"finish_reason\":\"COMPLETE\"}");
        item.offer("{\"event_type\":\"stream-end\", \"finish_reason\":\"STOP_SEQUENCE\"}");
        item.offer("{\"event_type\":\"stream-end\", \"finish_reason\":\"USER_CANCEL\"}");
        item.offer("{\"event_type\":\"stream-end\", \"finish_reason\":\"MAX_TOKENS\"}");

        var processor = new CohereStreamingProcessor();

        Flow.Subscriber<ChunkedToXContent> downstream = mock();
        processor.subscribe(downstream);

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);

        processor.next(item);

        verify(upstream, times(1)).request(1);
        verify(downstream, times(0)).onNext(any());
    }

    public void testParseCohereData() throws Exception {
        var item = new ArrayDeque<String>();
        item.offer("{\"event_type\":\"text-generation\", \"text\":\"hello there\"}");

        var processor = new CohereStreamingProcessor();

        Flow.Subscriber<ChunkedToXContent> downstream = mock();
        processor.subscribe(downstream);

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);

        processor.next(item);

        verify(upstream, times(0)).request(1);
        verify(downstream, times(1)).onNext(assertArg(chunks -> {
            assertThat(chunks, isA(StreamingChatCompletionResults.Results.class));
            var results = (StreamingChatCompletionResults.Results) chunks;
            assertThat(results.results().size(), equalTo(1));
            assertThat(results.results().getFirst().delta(), equalTo("hello there"));
        }));
    }
}
