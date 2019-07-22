/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.sillymodel;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.ml.inference.Model;
import org.elasticsearch.xpack.ml.inference.ModelLoader;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class SillyModelLoader implements ModelLoader {

    public static String TYPE = "silly";

    private final Client client;

    public SillyModelLoader(Client client) {
        this.client = client;
    }

    @Override
    public Model load(String id, String index) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Model> model = new AtomicReference<>();
        AtomicReference<Exception> exception = new AtomicReference<>();

        LatchedActionListener<Model> listener = new LatchedActionListener<>(
                ActionListener.wrap(model::set, exception::set), latch
        );

        load(id, index, listener);
        latch.await();
        if (exception.get() != null) {
            throw exception.get();
        }

        return model.get();
    }

    public void load(String id, String index, ActionListener<Model> listener) {
        client.prepareGet(id, null, index).execute(ActionListener.wrap(
                response -> {
                    if (response.isExists()) {
                        listener.onResponse(new SillyModel(response.getSourceAsBytesRef()));
                    } else {
                        listener.onFailure(new ResourceNotFoundException("missing model [{}], [{}]", id, index));
                    }
                },
                listener::onFailure
        ));
    }
}
