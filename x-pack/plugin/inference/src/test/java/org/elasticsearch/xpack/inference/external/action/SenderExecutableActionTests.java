/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.RequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class SenderExecutableActionTests extends ESTestCase {
    private static final String failedToSendRequestErrorMessage = "test failed";
    private static final String failureExceptionMessage = failedToSendRequestErrorMessage + ". Cause: test";
    private Sender sender;
    private RequestManager requestManager;
    private SenderExecutableAction executableAction;

    @Before
    public void setUpMocks() {
        sender = mock(Sender.class);
        requestManager = mock(RequestManager.class);
        executableAction = new SenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage);
    }

    public void testSendCompletesSuccessfully() {
        var testRan = new AtomicBoolean(false);

        mockSender(listener -> listener.onResponse(mock(InferenceServiceResults.class)));

        executableAction.execute(
            mock(InferenceInputs.class),
            mock(TimeValue.class),
            ActionListener.wrap(success -> testRan.set(true), e -> fail(e, "Test failed."))
        );

        assertTrue("Test failed to call listener.", testRan.get());
    }

    @SuppressWarnings("unchecked")
    public void testSendThrowingElasticsearchExceptionIsUnwrapped() {
        var expectedException = new ElasticsearchException("test");
        var actualException = new AtomicReference<Exception>();

        doThrow(expectedException).when(sender)
            .send(eq(requestManager), any(InferenceInputs.class), any(TimeValue.class), any(ActionListener.class));

        execute(actualException);

        assertThat(actualException.get(), notNullValue());
        assertThat(actualException.get(), sameInstance(expectedException));
    }

    public void testSenderReturnedElasticsearchExceptionIsUnwrapped() {
        var expectedException = new ElasticsearchException("test");
        var actualException = new AtomicReference<Exception>();

        mockSender(listener -> listener.onFailure(expectedException));

        execute(actualException);

        assertThat(actualException.get(), notNullValue());
        assertThat(actualException.get(), sameInstance(expectedException));
    }

    @SuppressWarnings("unchecked")
    public void testSendThrowingExceptionIsWrapped() {
        var expectedException = new IllegalStateException("test");
        var actualException = new AtomicReference<Exception>();

        doThrow(expectedException).when(sender)
            .send(eq(requestManager), any(InferenceInputs.class), any(TimeValue.class), any(ActionListener.class));

        execute(actualException);

        assertThat(actualException.get(), notNullValue());
        assertThat(actualException.get().getMessage(), is(failureExceptionMessage));
        assertThat(actualException.get(), instanceOf(ElasticsearchStatusException.class));
        assertThat(actualException.get().getCause(), sameInstance(expectedException));
    }

    public void testSenderReturnedExceptionIsWrapped() {
        var expectedException = new IllegalStateException("test");
        var actualException = new AtomicReference<Exception>();

        mockSender(listener -> listener.onFailure(expectedException));

        execute(actualException);

        assertThat(actualException.get(), notNullValue());
        assertThat(actualException.get().getMessage(), is(failureExceptionMessage));
        assertThat(actualException.get(), instanceOf(ElasticsearchStatusException.class));
        assertThat(actualException.get().getCause(), sameInstance(expectedException));
    }

    @SuppressWarnings("unchecked")
    private void mockSender(Consumer<ActionListener<InferenceServiceResults>> listener) {
        doAnswer(ans -> {
            listener.accept(ans.getArgument(3, ActionListener.class));
            return null; // void
        }).when(sender).send(eq(requestManager), any(InferenceInputs.class), any(TimeValue.class), any(ActionListener.class));
    }

    private void execute(AtomicReference<Exception> actualException) {
        executableAction.execute(
            mock(InferenceInputs.class),
            mock(TimeValue.class),
            ActionListener.wrap(shouldNotSucceed -> fail("Test failed."), actualException::set)
        );
    }

}
