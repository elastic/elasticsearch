/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.streaming;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Flow;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ServerSentEventProcessorTests extends ESTestCase {

    public void testEmptyBody() {
        var processor = new ServerSentEventProcessor(mock());

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);

        Flow.Subscriber<Deque<ServerSentEvent>> downstream = mock();
        processor.subscribe(downstream);

        processor.next(new HttpResult(mock(), new byte[0]));
        verify(upstream, times(1)).request(1);
        verify(downstream, times(0)).onNext(any());
    }

    public void testEmptyParseResponse() {
        ServerSentEventParser parser = mock();
        when(parser.parse(any())).thenReturn(new ArrayDeque<>());

        var processor = new ServerSentEventProcessor(parser);

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);

        Flow.Subscriber<Deque<ServerSentEvent>> downstream = mock();
        processor.subscribe(downstream);

        processor.next(new HttpResult(mock(), "hello".getBytes(StandardCharsets.UTF_8)));
        verify(upstream, times(1)).request(1);
        verify(downstream, times(0)).onNext(any());
    }

    public void testResponse() {
        ServerSentEventParser parser = mock();
        var deque = new ArrayDeque<ServerSentEvent>();
        deque.offer(new ServerSentEvent("hello"));
        when(parser.parse(any())).thenReturn(deque);

        var processor = new ServerSentEventProcessor(parser);

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);

        Flow.Subscriber<Deque<ServerSentEvent>> downstream = mock();
        processor.subscribe(downstream);

        processor.next(new HttpResult(mock(), "hello".getBytes(StandardCharsets.UTF_8)));
        verify(upstream, times(0)).request(anyLong());
        verify(downstream, times(1)).onNext(eq(deque));
    }
}
