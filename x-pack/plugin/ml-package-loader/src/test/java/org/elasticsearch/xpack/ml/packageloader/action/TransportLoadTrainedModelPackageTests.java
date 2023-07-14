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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.AuditMlNotificationAction;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfig;
import org.elasticsearch.xpack.core.ml.packageloader.action.LoadTrainedModelPackageAction;
import org.hamcrest.CoreMatchers;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransportLoadTrainedModelPackageTests extends ESTestCase {
    private static final String MODEL_IMPORT_FAILURE_MSG_FORMAT = "Model importing failed due to %s [%s]";

    public void testSendsFinishedUploadNotification() {
        var uploader = mock(ModelImporter.class);
        var taskManager = mock(TaskManager.class);
        var task = mock(Task.class);
        var client = mock(Client.class);

        TransportLoadTrainedModelPackage.importModel(client, taskManager, createRequest(true), uploader, ActionListener.noop(), task);

        var notificationArg = ArgumentCaptor.forClass(AuditMlNotificationAction.Request.class);
        verify(client).execute(eq(AuditMlNotificationAction.INSTANCE), notificationArg.capture(), any());
        assertThat(notificationArg.getValue().getMessage(), CoreMatchers.containsString("finished model import after"));
    }

    public void testSendsErrorNotificationForInternalError() throws URISyntaxException, IOException {
        ElasticsearchStatusException exception = new ElasticsearchStatusException("exception", RestStatus.INTERNAL_SERVER_ERROR);

        assertUploadCallsOnFailure(exception, exception.toString());
    }

    public void testSendsErrorNotificationForMalformedURL() throws URISyntaxException, IOException {
        MalformedURLException exception = new MalformedURLException("exception");
        String message = format(MODEL_IMPORT_FAILURE_MSG_FORMAT, "an invalid URL", exception.toString());

        assertUploadCallsOnFailure(exception, message, RestStatus.INTERNAL_SERVER_ERROR);
    }

    public void testSendsErrorNotificationForURISyntax() throws URISyntaxException, IOException {
        URISyntaxException exception = mock(URISyntaxException.class);
        String message = format(MODEL_IMPORT_FAILURE_MSG_FORMAT, "an invalid URL syntax", exception.toString());

        assertUploadCallsOnFailure(exception, message, RestStatus.INTERNAL_SERVER_ERROR);
    }

    public void testSendsErrorNotificationForIOException() throws URISyntaxException, IOException {
        IOException exception = mock(IOException.class);
        String message = format(MODEL_IMPORT_FAILURE_MSG_FORMAT, "an IOException", exception.toString());

        assertUploadCallsOnFailure(exception, message, RestStatus.SERVICE_UNAVAILABLE);
    }

    public void testSendsErrorNotificationForException() throws URISyntaxException, IOException {
        RuntimeException exception = mock(RuntimeException.class);
        String message = format(MODEL_IMPORT_FAILURE_MSG_FORMAT, "an Exception", exception.toString());

        assertUploadCallsOnFailure(exception, message, RestStatus.INTERNAL_SERVER_ERROR);
    }

    public void testCallsOnResponseWithAcknowledgedResponse() throws URISyntaxException, IOException {
        var client = mock(Client.class);
        var taskManager = mock(TaskManager.class);
        var task = mock(Task.class);
        ModelImporter uploader = createUploader(null);

        var responseRef = new AtomicReference<AcknowledgedResponse>();
        var l = ActionListener.wrap((AcknowledgedResponse response) -> {
            assertThat(response, is(AcknowledgedResponse.TRUE));
            responseRef.set(response);
        }, e -> fail("received an exception: " + e.getMessage()));

        TransportLoadTrainedModelPackage.importModel(client, taskManager, createRequest(true), uploader, l, task);
        assertThat(responseRef.get(), is(AcknowledgedResponse.TRUE));
    }

    public void testDoesNotCallListenerWhenNotWaitingForCompletion() {
        var uploader = mock(ModelImporter.class);
        var client = mock(Client.class);
        var taskManager = mock(TaskManager.class);
        var task = mock(Task.class);

        TransportLoadTrainedModelPackage.importModel(
            client,
            taskManager,
            createRequest(false),
            uploader,
            ActionListener.running(ESTestCase::fail),
            task
        );
    }

    private void assertUploadCallsOnFailure(Exception exception, String message, RestStatus status) throws URISyntaxException, IOException {
        var esStatusException = new ElasticsearchStatusException(message, status, exception);

        assertNotificationAndOnFailure(exception, esStatusException, message);
    }

    private void assertUploadCallsOnFailure(ElasticsearchStatusException exception, String message) throws URISyntaxException, IOException {
        assertNotificationAndOnFailure(exception, exception, message);
    }

    private void assertNotificationAndOnFailure(Exception thrownException, ElasticsearchStatusException onFailureException, String message)
        throws URISyntaxException, IOException {
        var client = mock(Client.class);
        var taskManager = mock(TaskManager.class);
        var task = mock(Task.class);
        ModelImporter uploader = createUploader(thrownException);

        var failureRef = new AtomicReference<Exception>();
        var listener = ActionListener.wrap((AcknowledgedResponse response) -> {
            assertThat(response, is(AcknowledgedResponse.TRUE));
            fail("received a acknowledged response: " + response.toString());
        }, failureRef::set);
        TransportLoadTrainedModelPackage.importModel(client, taskManager, createRequest(true), uploader, listener, task);

        var notificationArg = ArgumentCaptor.forClass(AuditMlNotificationAction.Request.class);
        verify(client).execute(eq(AuditMlNotificationAction.INSTANCE), notificationArg.capture(), any());
        assertThat(notificationArg.getValue().getMessage(), is(message));

        var receivedException = (ElasticsearchStatusException) failureRef.get();
        assertThat(receivedException.toString(), is(onFailureException.toString()));
        assertThat(receivedException.status(), is(onFailureException.status()));
        assertThat(receivedException.getCause(), is(onFailureException.getCause()));
    }

    private ModelImporter createUploader(Exception exception) throws URISyntaxException, IOException {
        ModelImporter uploader = mock(ModelImporter.class);
        if (exception != null) {
            doThrow(exception).when(uploader).doImport();
        }

        return uploader;
    }

    private LoadTrainedModelPackageAction.Request createRequest(boolean waitForCompletion) {
        return new LoadTrainedModelPackageAction.Request("id", mock(ModelPackageConfig.class), waitForCompletion);
    }
}
