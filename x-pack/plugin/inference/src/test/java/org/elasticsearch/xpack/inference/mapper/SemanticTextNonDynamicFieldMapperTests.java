/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.mapper.NonDynamicFieldMapperTests;
import org.elasticsearch.inference.Model;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SemanticTextNonDynamicFieldMapperTests extends NonDynamicFieldMapperTests {

    @Before
    public void setup() throws Exception {
        Utils.storeSparseModel(client());
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(Utils.TestInferencePlugin.class);
    }

    @Override
    protected String getTypeName() {
        return SemanticTextFieldMapper.CONTENT_TYPE;
    }

    @Override
    protected String getMapping() {
        return String.format(Locale.ROOT, """
            "type": "%s",
            "inference_id": "%s"
            """, SemanticTextFieldMapper.CONTENT_TYPE, TestSparseInferenceServiceExtension.TestInferenceService.NAME);
    }

    private void storeSparseModel() throws Exception {
        Model model = new TestSparseInferenceServiceExtension.TestSparseModel(
            TestSparseInferenceServiceExtension.TestInferenceService.NAME,
            new TestSparseInferenceServiceExtension.TestServiceSettings("sparse_model", null, false)
        );
        storeModel(model);
    }

    private void storeModel(Model model) throws Exception {
        ModelRegistry modelRegistry = new ModelRegistry(client());

        AtomicReference<Boolean> storeModelHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> modelRegistry.storeModel(model, listener), storeModelHolder, exceptionHolder);

        assertThat(storeModelHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));
    }

    private <T> void blockingCall(Consumer<ActionListener<T>> function, AtomicReference<T> response, AtomicReference<Exception> error)
        throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<T> listener = ActionListener.wrap(r -> {
            response.set(r);
            latch.countDown();
        }, e -> {
            error.set(e);
            latch.countDown();
        });

        function.accept(listener);
        latch.await();
    }
}
