/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.packageloader.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.AuditMlNotificationAction;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfigTests;
import org.elasticsearch.xpack.core.ml.packageloader.action.LoadTrainedModelPackageAction;
import org.hamcrest.CoreMatchers;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Before;

public class TransportLoadTrainedModelPackageTests extends ESTestCase {
    private LoadTrainedModelPackageAction.Request loadModelReq;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        loadModelReq = createRequest("abc");
    }

    private LoadTrainedModelPackageAction.Request createRequest(String vocabFile) {
        ModelPackageConfig config = new ModelPackageConfig.Builder(ModelPackageConfigTests.randomModulePackageConfig()).setVocabularyFile(
            vocabFile
        ).build();

        return new LoadTrainedModelPackageAction.Request("id1", config, false);
    }

    public void testSendsFinishedUploadNotification() {
        LoadTrainedModelPackageAction.Request req = createRequest(null);
        ModelUploader uploader = mock(ModelUploader.class);
        Client client = mock(Client.class);

        TransportLoadTrainedModelPackage.uploadModel(client, req, uploader, ActionListener.noop());

        ArgumentCaptor<AuditMlNotificationAction.Request> argument = ArgumentCaptor.forClass(AuditMlNotificationAction.Request.class);
        verify(client).execute(eq(AuditMlNotificationAction.INSTANCE), argument.capture(), any());
        assertThat(argument.getValue().getMessage(), CoreMatchers.containsString("finished model upload after"));
    }

    public void testSendsErrorNotificationForInternalError() throws URISyntaxException, IOException {
        ElasticsearchStatusException exception = new ElasticsearchStatusException("exception", RestStatus.INTERNAL_SERVER_ERROR);

        assertUploadCallsOnFailure(exception, "exception");
    }

    private void assertUploadCallsOnFailure(Exception exception, String message) throws URISyntaxException, IOException {
        Client client = mock(Client.class);
        ModelUploader uploader = createUploader(exception);

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) mock(ActionListener.class);
        TransportLoadTrainedModelPackage.uploadModel(client, loadModelReq, uploader, listener);

        ArgumentCaptor<AuditMlNotificationAction.Request> argument = ArgumentCaptor.forClass(AuditMlNotificationAction.Request.class);
        verify(client).execute(eq(AuditMlNotificationAction.INSTANCE), argument.capture(), any());
        assertThat(argument.getValue().getMessage(), CoreMatchers.containsString(message));

        verify(listener).onFailure(exception);
    }

    private ModelUploader createUploader(Exception exception) throws URISyntaxException, IOException {
        ModelUploader uploader = mock(ModelUploader.class);
        if (exception != null) {
            doThrow(exception).when(uploader).upload();
        }

        return uploader;
    }

    public void testSendsErrorNotificationForMalformedURL() throws URISyntaxException, IOException {
        MalformedURLException exception = new MalformedURLException("exception");

        assertUploadCallsOnFailure(exception, "Invalid URL");
    }

    public void testSendsErrorNotificationForURISyntax() throws URISyntaxException, IOException {
        URISyntaxException exception = mock(URISyntaxException.class);

        assertUploadCallsOnFailure(exception, "Invalid URL syntax");
    }

    public void testSendsErrorNotificationForIOException() throws URISyntaxException, IOException {
        IOException exception = mock(IOException.class);

        assertUploadCallsOnFailure(exception, "IOException");
    }

    public void testSendsErrorNotificationForException() throws URISyntaxException, IOException {
        RuntimeException exception = mock(RuntimeException.class);

        assertUploadCallsOnFailure(exception, "Exception");
    }

    public void testCallsOnResponseWithAcknowledgedResponse() throws URISyntaxException, IOException {
        Client client = mock(Client.class);
        ModelUploader uploader = createUploader(null);

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) mock(ActionListener.class);
        TransportLoadTrainedModelPackage.uploadModel(client, loadModelReq, uploader, listener);

        verify(listener).onResponse(AcknowledgedResponse.TRUE);
    }
}
