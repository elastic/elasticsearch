/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.RequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SingleInputSenderExecutableActionTests extends ESTestCase {
    private static final String errorMessage = "test";
    private SingleInputSenderExecutableAction executableAction;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpMocks() {
        var sender = mock(Sender.class);
        var requestManager = mock(RequestManager.class);
        executableAction = new SingleInputSenderExecutableAction(sender, requestManager, errorMessage, errorMessage);

        doAnswer(ans -> {
            ans.getArgument(3, ActionListener.class).onResponse(mock(InferenceServiceResults.class));
            return null; // void
        }).when(sender).send(eq(requestManager), any(InferenceInputs.class), any(TimeValue.class), any(ActionListener.class));
    }

    public void testOneInputIsValid() {
        var testRan = new AtomicBoolean(false);

        executableAction.execute(
            new UnifiedChatInput(List.of("one"), "system", false),
            mock(TimeValue.class),
            ActionListener.wrap(success -> testRan.set(true), e -> fail(e, "Test failed."))
        );

        assertTrue("Test failed to call listener.", testRan.get());
    }

    public void testMoreThanOneInput() {
        var badInput = mock(EmbeddingsInput.class);
        var input = List.of(new ChunkInferenceInput("one"), new ChunkInferenceInput("two"));
        when(badInput.getInputs()).thenReturn(input);
        when(badInput.isSingleInput()).thenReturn(false);
        var actualException = new AtomicReference<Exception>();

        executableAction.execute(
            badInput,
            mock(TimeValue.class),
            ActionListener.wrap(shouldNotSucceed -> fail("Test failed."), actualException::set)
        );

        assertThat(actualException.get(), notNullValue());
        assertThat(actualException.get().getMessage(), is("test only accepts 1 input"));
        assertThat(actualException.get(), instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) actualException.get()).status(), is(RestStatus.BAD_REQUEST));
    }
}
