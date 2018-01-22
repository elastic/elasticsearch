/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.junit.Before;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PreviewDatafeedActionTests extends ESTestCase {

    private DataExtractor dataExtractor;
    private ActionListener<PreviewDatafeedAction.Response> actionListener;
    private String capturedResponse;
    private Exception capturedFailure;

    @Before
    public void setUpTests() {
        dataExtractor = mock(DataExtractor.class);
        actionListener = mock(ActionListener.class);

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                PreviewDatafeedAction.Response response = (PreviewDatafeedAction.Response) invocationOnMock.getArguments()[0];
                capturedResponse = response.toString();
                return null;
            }
        }).when(actionListener).onResponse(any());

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                capturedFailure = (Exception) invocationOnMock.getArguments()[0];
                return null;
            }
        }).when(actionListener).onFailure(any());
    }

    public void testPreviewDatafed_GivenEmptyStream() throws IOException {
        when(dataExtractor.next()).thenReturn(Optional.empty());

        TransportPreviewDatafeedAction.previewDatafeed(dataExtractor, actionListener);

        assertThat(capturedResponse, equalTo("[]"));
        assertThat(capturedFailure, is(nullValue()));
        verify(dataExtractor).cancel();
    }

    public void testPreviewDatafed_GivenNonEmptyStream() throws IOException {
        String streamAsString = "{\"a\":1, \"b\":2} {\"c\":3, \"d\":4}\n{\"e\":5, \"f\":6}";
        InputStream stream = new ByteArrayInputStream(streamAsString.getBytes(StandardCharsets.UTF_8));
        when(dataExtractor.next()).thenReturn(Optional.of(stream));

        TransportPreviewDatafeedAction.previewDatafeed(dataExtractor, actionListener);

        assertThat(capturedResponse, equalTo("[{\"a\":1, \"b\":2},{\"c\":3, \"d\":4},{\"e\":5, \"f\":6}]"));
        assertThat(capturedFailure, is(nullValue()));
        verify(dataExtractor).cancel();
    }

    public void testPreviewDatafed_GivenFailure() throws IOException {
        doThrow(new RuntimeException("failed")).when(dataExtractor).next();

        TransportPreviewDatafeedAction.previewDatafeed(dataExtractor, actionListener);

        assertThat(capturedResponse, is(nullValue()));
        assertThat(capturedFailure.getMessage(), equalTo("failed"));
        verify(dataExtractor).cancel();
    }
}