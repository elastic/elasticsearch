/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.utils;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

// TODO (#127): norelease Placeholder fix until: https://github.com/elastic/prelert-legacy/issues/127 gets in.
public class FixBlockingClientOperations {

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static  <Res extends ActionResponse> Res executeBlocking(Client client, Action action, ActionRequest request) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Res> response = new AtomicReference<>();
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<Res> listener = new ActionListener<Res>() {
            @Override
            public void onResponse(Res r) {
                response.set(r);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exception.set(e);
                latch.countDown();
            }
        };
        client.execute(action, request, listener);
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (exception.get() != null) {
            throw org.elasticsearch.ExceptionsHelper.convertToElastic(exception.get());
        } else {
            return response.get();
        }
    }

}
