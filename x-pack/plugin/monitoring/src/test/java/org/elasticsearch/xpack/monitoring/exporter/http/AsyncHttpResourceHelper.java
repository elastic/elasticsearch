/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.monitoring.exporter.http;

import java.util.List;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.hamcrest.Matcher;
import org.mockito.stubbing.Stubber;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class AsyncHttpResourceHelper {

    @SuppressWarnings("unchecked")
    static ActionListener<Boolean> mockBooleanActionListener() {
        return mock(ActionListener.class);
    }

    @SuppressWarnings("unchecked")
    static ActionListener<HttpResource.ResourcePublishResult> mockPublishResultActionListener() {
        return mock(ActionListener.class);
    }

    static <T> ActionListener<T> wrapMockListener(ActionListener<T> mock) {
        // wraps the mock listener so that default functions on the ActionListener interface can be used
        return ActionListener.wrap(mock::onResponse, mock::onFailure);
    }

    static void whenPerformRequestAsyncWith(final RestClient client, final Response response) {
        doAnswer(invocation -> {
            ((ResponseListener)invocation.getArguments()[1]).onSuccess(response);
            return null;
        }).when(client).performRequestAsync(any(Request.class), any(ResponseListener.class));
    }

    static void whenPerformRequestAsyncWith(final RestClient client, final Matcher<Request> request, final Response response) {
        doAnswer(invocation -> {
            ((ResponseListener)invocation.getArguments()[1]).onSuccess(response);
            return null;
        }).when(client).performRequestAsync(argThat(request), any(ResponseListener.class));
    }

    static void whenPerformRequestAsyncWith(final RestClient client, final Matcher<Request> request, final List<Response> responses) {
        if (responses.size() == 1) {
            whenPerformRequestAsyncWith(client, request, responses.get(0));
        } else if (responses.size() > 1) {
            whenPerformRequestAsyncWith(client, request, responses.get(0), responses.subList(1, responses.size()), null);
        }
    }

    static void whenPerformRequestAsyncWith(final RestClient client,
                                            final Matcher<Request> request,
                                            final Response response,
                                            final Exception exception) {
        whenPerformRequestAsyncWith(client, request, response, null, exception);
    }

    static void whenPerformRequestAsyncWith(final RestClient client,
                                            final Matcher<Request> request,
                                            final Response first,
                                            final List<Response> responses,
                                            final Exception exception) {
        Stubber stub = doAnswer(invocation -> {
            ((ResponseListener)invocation.getArguments()[1]).onSuccess(first);
            return null;
        });

        if (responses != null) {
            for (final Response response : responses) {
                stub.doAnswer(invocation -> {
                    ((ResponseListener) invocation.getArguments()[1]).onSuccess(response);
                    return null;
                });
            }
        }

        if (exception != null) {
            stub.doAnswer(invocation -> {
                ((ResponseListener) invocation.getArguments()[1]).onFailure(exception);
                return null;
            });
        }

        stub.when(client).performRequestAsync(argThat(request), any(ResponseListener.class));
    }

    static void whenPerformRequestAsyncWith(final RestClient client, final Request request, final Response response) {
        doAnswer(invocation -> {
            ((ResponseListener)invocation.getArguments()[1]).onSuccess(response);
            return null;
        }).when(client).performRequestAsync(eq(request), any(ResponseListener.class));
    }

    static void whenPerformRequestAsyncWith(final RestClient client, final Exception exception) {
        doAnswer(invocation -> {
            ((ResponseListener)invocation.getArguments()[1]).onFailure(exception);
            return null;
        }).when(client).performRequestAsync(any(Request.class), any(ResponseListener.class));
    }

    static void whenPerformRequestAsyncWith(final RestClient client, final Matcher<Request> request, final Exception exception) {
        doAnswer(invocation -> {
            ((ResponseListener)invocation.getArguments()[1]).onFailure(exception);
            return null;
        }).when(client).performRequestAsync(argThat(request), any(ResponseListener.class));
    }

    static void whenPerformRequestAsyncWith(final RestClient client, final Request request, final Exception exception) {
        doAnswer(invocation -> {
            ((ResponseListener)invocation.getArguments()[1]).onFailure(exception);
            return null;
        }).when(client).performRequestAsync(eq(request), any(ResponseListener.class));
    }

}
