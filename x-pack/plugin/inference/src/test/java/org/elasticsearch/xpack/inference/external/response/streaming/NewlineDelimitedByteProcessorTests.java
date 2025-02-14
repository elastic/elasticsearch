/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.streaming;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.concurrent.Flow;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class NewlineDelimitedByteProcessorTests extends ESTestCase {
    private Flow.Subscription upstream;
    private Flow.Subscriber<Deque<String>> downstream;
    private NewlineDelimitedByteProcessor processor;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        upstream = mock();
        downstream = mock();
        processor = new NewlineDelimitedByteProcessor();
        processor.onSubscribe(upstream);
        processor.subscribe(downstream);
    }

    public void testEmptyBody() {
        processor.next(result(null));
        processor.onComplete();
        verify(upstream, times(1)).request(1);
        verify(downstream, times(0)).onNext(any());
    }

    private HttpResult result(String response) {
        return new HttpResult(mock(), response == null ? new byte[0] : response.getBytes(StandardCharsets.UTF_8));
    }

    public void testEmptyParseResponse() {
        processor.next(result(""));
        verify(upstream, times(1)).request(1);
        verify(downstream, times(0)).onNext(any());
    }

    public void testValidResponse() {
        processor.next(result("{\"hello\":\"there\"}\n"));
        verify(downstream, times(1)).onNext(assertArg(deque -> {
            assertThat(deque, notNullValue());
            assertThat(deque.size(), is(1));
            assertThat(deque.getFirst(), is("{\"hello\":\"there\"}"));
        }));
    }

    public void testMultipleValidResponse() {
        processor.next(result("""
            {"value": 1}
            {"value": 2}
            {"value": 3}
            """));
        verify(upstream, times(0)).request(1);
        verify(downstream, times(1)).onNext(assertArg(deque -> {
            assertThat(deque, notNullValue());
            assertThat(deque.size(), is(3));
            var items = deque.iterator();
            IntStream.range(1, 4).forEach(i -> {
                assertThat(items.hasNext(), is(true));
                assertThat(items.next(), containsString(String.valueOf(i)));
            });
        }));
    }

    public void testOnCompleteFlushesResponse() {
        processor.next(result("""
            {"value": 1}"""));

        // onNext should not be called with only one value
        verify(downstream, times(0)).onNext(any());
        verify(downstream, times(0)).onComplete();

        // onComplete should flush the value pending, and onNext should be called
        processor.onComplete();
        verify(downstream, times(1)).onNext(assertArg(deque -> {
            assertThat(deque, notNullValue());
            assertThat(deque.size(), is(1));
            var item = deque.getFirst();
            assertThat(item, containsString(String.valueOf(1)));
        }));
        verify(downstream, times(0)).onComplete();

        // next time the downstream requests data, onComplete is called
        var downstreamSubscription = ArgumentCaptor.forClass(Flow.Subscription.class);
        verify(downstream).onSubscribe(downstreamSubscription.capture());
        downstreamSubscription.getValue().request(1);
        verify(downstream, times(1)).onComplete();
    }
}
