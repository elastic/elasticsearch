/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.inference.UnparsedModel;

import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class ModelRegistryEisInvalidUrlIT extends ModelRegistryEisBase {
    public ModelRegistryEisInvalidUrlIT() {
        super("");
    }

    public void testGetAllModelsDoesNotReturnEisModels_WhenEisUrlIsEmpty() {
        initializeModels();

        PlainActionFuture<List<UnparsedModel>> listener = new PlainActionFuture<>();
        modelRegistry.getAllModels(false, listener);

        var results = listener.actionGet(TIMEOUT);
        var expected = Stream.of("sparse-1", "sparse-2", "sparse-3", "embedding-1", "embedding-2").toArray(String[]::new);
        assertThat(results.size(), is(expected.length));
        assertThat(results.stream().map(UnparsedModel::inferenceEntityId).sorted().toList(), containsInAnyOrder(expected));
    }
}
