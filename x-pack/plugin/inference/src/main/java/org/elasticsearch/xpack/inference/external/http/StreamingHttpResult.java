/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.http.HttpResponse;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.rest.RestStatus;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

public record StreamingHttpResult(HttpResponse response, Flow.Publisher<byte[]> body) {
    public boolean isSuccessfulResponse() {
        return RestStatus.isSuccessful(response.getStatusLine().getStatusCode());
    }

    public Flow.Publisher<HttpResult> toHttpResult() {
        return subscriber -> body().subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscriber.onSubscribe(subscription);
            }

            @Override
            public void onNext(byte[] item) {
                subscriber.onNext(new HttpResult(response(), item));
            }

            @Override
            public void onError(Throwable throwable) {
                subscriber.onError(throwable);
            }

            @Override
            public void onComplete() {
                subscriber.onComplete();
            }
        });
    }

    public void readFullResponse(ActionListener<HttpResult> fullResponse) {
        var stream = new ByteArrayOutputStream();
        AtomicReference<Flow.Subscription> upstream = new AtomicReference<>(null);
        body.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                upstream.set(subscription);
                upstream.get().request(1);
            }

            @Override
            public void onNext(byte[] item) {
                stream.writeBytes(item);
                upstream.get().request(1);
            }

            @Override
            public void onError(Throwable throwable) {
                ExceptionsHelper.maybeError(throwable).ifPresent(ExceptionsHelper::maybeDieOnAnotherThread);
                fullResponse.onFailure(new RuntimeException("Fatal while fully consuming stream", throwable));
            }

            @Override
            public void onComplete() {
                fullResponse.onResponse(new HttpResult(response, stream.toByteArray()));
            }
        });
    }
}
