/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.inference.ModelMeta;
import org.elasticsearch.xpack.ml.inference.ModelMetadataManager;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class ModelMetadataManagerIT extends MlSingleNodeTestCase {

    private ModelMetadataManager modelMetadataManager;

    @Before
    public void createComponents() throws Exception {
        modelMetadataManager = new ModelMetadataManager(client(), xContentRegistry());
        waitForMlTemplates();
    }

    public void testCreateReadDelete() throws InterruptedException {

        ModelMeta modelMeta = new ModelMeta(randomAlphaOfLength(6), randomAlphaOfLength(6));

        AtomicReference<IndexResponse> indexResponseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        blockingCall(listener -> modelMetadataManager.putModelMeta(modelMeta, listener), indexResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertEquals(RestStatus.CREATED, indexResponseHolder.get().status());

        AtomicReference<ModelMeta> modelMetaHolder = new AtomicReference<>();
        blockingCall(listener -> modelMetadataManager.getModelMeta(modelMeta.getModelId(), listener), modelMetaHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertEquals(modelMeta, modelMetaHolder.get());

        AtomicReference<DeleteResponse> deleteResponseHolder = new AtomicReference<>();
        blockingCall(listener -> modelMetadataManager.deleteModelMeta(modelMeta.getModelId(), listener),
                deleteResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponseHolder.get().getResult());
    }

    public void testCannotCreateTwice() throws InterruptedException {
        ModelMeta modelMeta = new ModelMeta("same-id", randomAlphaOfLength(6));

        AtomicReference<IndexResponse> indexResponseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        blockingCall(listener -> modelMetadataManager.putModelMeta(modelMeta, listener), indexResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertEquals(RestStatus.CREATED, indexResponseHolder.get().status());

        indexResponseHolder.set(null);
        blockingCall(listener -> modelMetadataManager.putModelMeta(modelMeta, listener), indexResponseHolder, exceptionHolder);
        assertNotNull(exceptionHolder.get());
        assertNull(indexResponseHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ResourceAlreadyExistsException.class));
        assertEquals("The model cannot be created with the Id [same-id]. The Id is already used.", exceptionHolder.get().getMessage());
    }

    public void testMissingDocument() throws InterruptedException {

        // first put anything to create the index otherwise the
        // GETs will cause a IndexNotFoundException
        AtomicReference<IndexResponse> indexResponseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        blockingCall(listener -> modelMetadataManager.putModelMeta(new ModelMeta("unrelated", "config"), listener),
                indexResponseHolder, exceptionHolder);
        assertEquals(RestStatus.CREATED, indexResponseHolder.get().status());

        AtomicReference<ModelMeta> modelMetaHolder = new AtomicReference<>();
        blockingCall(listener -> modelMetadataManager.getModelMeta("missing", listener), modelMetaHolder, exceptionHolder);
        assertNull(modelMetaHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ResourceNotFoundException.class));
        assertEquals("No model with id [missing] found", exceptionHolder.get().getMessage());

        exceptionHolder.set(null);
        AtomicReference<DeleteResponse> deleteResponseHolder = new AtomicReference<>();
        blockingCall(listener -> modelMetadataManager.deleteModelMeta("missing", listener), deleteResponseHolder, exceptionHolder);
        assertNull(deleteResponseHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ResourceNotFoundException.class));
        assertEquals("No model with id [missing] found", exceptionHolder.get().getMessage());
    }

    public void testGetMissingDocumentIndexNotCreated() throws InterruptedException {
        // the .ml-config does not exist this checks that the
        // subsequent IndexNotFoundException is converted to ResourceNotFoundException
        AtomicReference<ModelMeta> modelMetaHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        blockingCall(listener -> modelMetadataManager.getModelMeta("missing", listener), modelMetaHolder, exceptionHolder);
        assertThat(exceptionHolder.get(), instanceOf(ResourceNotFoundException.class));
    }
}
