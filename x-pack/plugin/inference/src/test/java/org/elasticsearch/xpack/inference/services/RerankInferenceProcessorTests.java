/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.chunking.RerankRequestChunker;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIService;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.LongDocumentStrategy;
import org.elasticsearch.xpack.inference.services.settings.RerankServiceSettings;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RerankInferenceProcessorTests extends ESTestCase {
    @Mock
    private SenderService mockRerankingSenderService;

    @Mock
    private Model mockModel;

    @Mock
    private ServiceSettings mockServiceSettings;

    @Mock
    private ExecutableAction mockExecutableAction;

    @Mock
    private QueryAndDocsInputs mockQueryAndDocsInputs;

    @Mock
    private TimeValue mockTimeValue;

    @Mock
    private ActionListener<InferenceServiceResults> mockListener;

    @SuppressWarnings("unchecked")
    @Before
    public void init() {
        MockitoAnnotations.openMocks(this);
        mockRerankingSenderService = mock(JinaAIService.class);
        mockModel = mock(Model.class);
        mockServiceSettings = mock(JinaAIRerankServiceSettings.class);
        mockExecutableAction = mock(ExecutableAction.class);
        mockQueryAndDocsInputs = mock(QueryAndDocsInputs.class);
        mockTimeValue = mock(TimeValue.class);
        mockListener = mock(ActionListener.class);

        when(mockModel.getServiceSettings()).thenReturn(mockServiceSettings);
    }

    public void testDoInfer_ModelDoesNotHaveRerankServiceSettings_ThrowsIllegalArgumentException() {
        when(mockModel.getServiceSettings()).thenReturn(mock(ServiceSettings.class));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            RerankInferenceProcessor.doInfer(
                mockRerankingSenderService,
                mockModel,
                mockExecutableAction,
                mockQueryAndDocsInputs,
                mockTimeValue,
                mockListener
            );
        });
        assertEquals("RerankInferenceProcessor can only process models with RerankServiceSettings", exception.getMessage());

        verify(mockModel).getServiceSettings();
        verifyNoMoreInteractionsOnMocks();
    }

    public void testDoInfer_LongDocumentStrategyNull_ExecutesActionWithoutChunking() {
        when(((RerankServiceSettings) mockServiceSettings).getLongDocumentStrategy()).thenReturn(null);

        RerankInferenceProcessor.doInfer(
            mockRerankingSenderService,
            mockModel,
            mockExecutableAction,
            mockQueryAndDocsInputs,
            mockTimeValue,
            mockListener
        );
        verify(mockModel).getServiceSettings();
        verify(mockExecutableAction).execute(mockQueryAndDocsInputs, mockTimeValue, mockListener);
        verifyNoMoreInteractionsOnMocks();
    }

    public void testDoInfer_LongDocumentStrategyTruncate_ExecutesActionWithoutChunk() {
        when(((RerankServiceSettings) mockServiceSettings).getLongDocumentStrategy()).thenReturn(LongDocumentStrategy.TRUNCATE);

        RerankInferenceProcessor.doInfer(
            mockRerankingSenderService,
            mockModel,
            mockExecutableAction,
            mockQueryAndDocsInputs,
            mockTimeValue,
            mockListener
        );
        verify(mockModel).getServiceSettings();
        verify(mockExecutableAction).execute(mockQueryAndDocsInputs, mockTimeValue, mockListener);
        verifyNoMoreInteractionsOnMocks();
    }

    public void testDoInfer_LongDocumentStrategyChunkAndInputsAreNotQueryAndDocsInputs_ThrowIllegalArgumentException() {
        when(((RerankServiceSettings) mockServiceSettings).getLongDocumentStrategy()).thenReturn(LongDocumentStrategy.CHUNK);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            RerankInferenceProcessor.doInfer(
                mockRerankingSenderService,
                mockModel,
                mockExecutableAction,
                mock(InferenceInputs.class),
                mockTimeValue,
                mockListener
            );
        });
        assertEquals("RerankInferenceProcessor can only process QueryAndDocsInputs when chunking is enabled", exception.getMessage());

        verify(mockModel).getServiceSettings();
        verifyNoMoreInteractionsOnMocks();
    }

    public void testDoInfer_LongDocumentStrategyChunkWithQueryAndDocsInputsAndNonRerankingInferenceService_ThrowIllegalArgumentException() {
        when(((RerankServiceSettings) mockServiceSettings).getLongDocumentStrategy()).thenReturn(LongDocumentStrategy.CHUNK);

        var mockQueryAndDocsInputs = mock(QueryAndDocsInputs.class);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            RerankInferenceProcessor.doInfer(
                mock(SenderService.class),
                mockModel,
                mockExecutableAction,
                mockQueryAndDocsInputs,
                mockTimeValue,
                mockListener
            );
        });
        assertEquals(
            "RerankInferenceProcessor can only process RerankingInferenceService when chunking is enabled",
            exception.getMessage()
        );

        verify(mockModel).getServiceSettings();
        verifyNoMoreInteractionsOnMocks();
    }

    @SuppressWarnings("unchecked")
    public void testDoInfer_LongDocumentStrategyChunkWithQueryAndDocsInputsAndRerankingInferenceService_ExecutesActionWithChunking() {
        when(((RerankServiceSettings) mockServiceSettings).getLongDocumentStrategy()).thenReturn(LongDocumentStrategy.CHUNK);
        var maxChunksPerDoc = randomIntBetween(1, 5);
        when(((RerankServiceSettings) mockServiceSettings).getMaxChunksPerDoc()).thenReturn(maxChunksPerDoc);
        QueryAndDocsInputs queryAndDocsInputs = new QueryAndDocsInputs(
            "This is a test query",
            List.of("This is the first chunk", "This is the second chunk", "This is the third chunk"),
            randomBoolean() ? randomBoolean() : null,
            randomBoolean() ? randomIntBetween(1, 10) : null,
            false
        );

        var rerankerWindowSize = randomIntBetween(512, 3000);
        when(((RerankingInferenceService) mockRerankingSenderService).rerankerWindowSize(any())).thenReturn(rerankerWindowSize);
        RerankInferenceProcessor.doInfer(
            mockRerankingSenderService,
            mockModel,
            mockExecutableAction,
            queryAndDocsInputs,
            mockTimeValue,
            mockListener
        );

        verify(mockModel).getServiceSettings();
        verify((RerankingInferenceService) mockRerankingSenderService).rerankerWindowSize(any());

        RerankRequestChunker rerankRequestChunker = new RerankRequestChunker(
            queryAndDocsInputs.getQuery(),
            queryAndDocsInputs.getChunks(),
            rerankerWindowSize,
            maxChunksPerDoc
        );

        QueryAndDocsInputs expectedChunkedInputs = new QueryAndDocsInputs(
            queryAndDocsInputs.getQuery(),
            rerankRequestChunker.getChunkedInputs(),
            queryAndDocsInputs.getReturnDocuments(),
            queryAndDocsInputs.getTopN(),
            queryAndDocsInputs.stream()
        );
        ArgumentCaptor<QueryAndDocsInputs> inputsCaptor = ArgumentCaptor.forClass(QueryAndDocsInputs.class);
        verify(mockExecutableAction).execute(inputsCaptor.capture(), eq(mockTimeValue), any(ActionListener.class));
        QueryAndDocsInputs capturedInputs = inputsCaptor.getValue();
        assertEquals(expectedChunkedInputs.getQuery(), capturedInputs.getQuery());
        assertEquals(expectedChunkedInputs.getChunks(), capturedInputs.getChunks());
        assertEquals(expectedChunkedInputs.getReturnDocuments(), capturedInputs.getReturnDocuments());
        assertEquals(expectedChunkedInputs.getTopN(), capturedInputs.getTopN());
        verifyNoMoreInteractionsOnMocks();
    }

    private void verifyNoMoreInteractionsOnMocks() {
        verifyNoMoreInteractions(
            mockRerankingSenderService,
            mockModel,
            mockExecutableAction,
            mockQueryAndDocsInputs,
            mockTimeValue,
            mockListener
        );
    }
}
