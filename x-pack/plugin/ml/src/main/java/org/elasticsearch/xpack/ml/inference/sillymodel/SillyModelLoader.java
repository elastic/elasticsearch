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
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.xpack.ml.inference.InferenceProcessor;
import org.elasticsearch.xpack.ml.inference.Model;
import org.elasticsearch.xpack.ml.inference.ModelLoader;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class SillyModelLoader implements ModelLoader {


    private static String INDEX = "index";

    private final Client client;

    public SillyModelLoader(Client client) {
        this.client = client;
    }

    @Override
    public Model load(String modelId, String processorTag, boolean ignoreMissing, Map<String, Object> config) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Model> model = new AtomicReference<>();
        AtomicReference<Exception> exception = new AtomicReference<>();

        LatchedActionListener<Model> listener = new LatchedActionListener<>(
                ActionListener.wrap(model::set, exception::set), latch
        );

        String index = ConfigurationUtils.readStringProperty(InferenceProcessor.TYPE, processorTag, config, INDEX);

        load(modelId, index, listener);
        latch.await();
        if (exception.get() != null) {
            throw exception.get();
        }

        return model.get();
    }

    @Override
    public void readConfiguration(String processorTag, Map<String, Object> config) {
        ConfigurationUtils.readStringProperty(InferenceProcessor.TYPE, processorTag, config, INDEX);
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
