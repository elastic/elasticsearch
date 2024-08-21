/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.packageloader.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelDefinitionPartAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelVocabularyAction;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfig;
import org.elasticsearch.xpack.ml.packageloader.MachineLearningPackageLoader;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ModelImporterTests extends ESTestCase {

    private TestThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = createThreadPool(MachineLearningPackageLoader.modelDownloadExecutor(Settings.EMPTY));
    }

    @After
    public void closeThreadPool() {
        threadPool.close();
    }

    public void testDownload() throws InterruptedException {
        var client = mockClient(false);
        var task = ModelDownloadTaskTests.testTask();
        var config = mock(ModelPackageConfig.class);
        var vocab = new ModelLoaderUtils.VocabularyParts(List.of(), List.of(), List.of());

        var importer = new ModelImporter(client, "foo", config, task, threadPool);
        int totalParts = 5;
        int chunkSize = 10;
        long size = totalParts * chunkSize;
        var modelDef = modelDefinition(totalParts, chunkSize);
        var stream = mockStreamChunker(modelDef, totalParts, chunkSize);

        var digest = computeDigest(modelDef, totalParts, chunkSize);
        when(config.getSha256()).thenReturn(digest);
        when(config.getSize()).thenReturn(size);

        var latch = new CountDownLatch(1);
        var latchedListener = new LatchedActionListener<AcknowledgedResponse>(ActionTestUtils.assertNoFailureListener(ignore -> {}), latch);
        importer.downloadParts(stream, size, vocab, latchedListener);

        latch.await();
        verify(client, times(totalParts)).execute(eq(PutTrainedModelDefinitionPartAction.INSTANCE), any(), any());
        assertEquals(totalParts, task.getStatus().downloadProgress().downloadedParts());
        assertEquals(totalParts, task.getStatus().downloadProgress().totalParts());
    }

    public void testSizeMismatch() throws InterruptedException {
        var client = mockClient(false);
        var task = mock(ModelDownloadTask.class);
        var config = mock(ModelPackageConfig.class);

        var importer = new ModelImporter(client, "foo", config, task, threadPool);
        int totalParts = 5;
        int chunkSize = 10;
        long size = totalParts * chunkSize;
        var modelDef = modelDefinition(totalParts, chunkSize);
        var stream = mockStreamChunker(modelDef, totalParts, chunkSize);

        var digest = computeDigest(modelDef, totalParts, chunkSize);
        when(config.getSha256()).thenReturn(digest);
        when(config.getSize()).thenReturn(size - 1); // expected size and read size are different

        var exceptionHolder = new AtomicReference<Exception>();

        var latch = new CountDownLatch(1);
        var latchedListener = new LatchedActionListener<AcknowledgedResponse>(
            ActionTestUtils.assertNoSuccessListener(exceptionHolder::set),
            latch
        );
        importer.downloadParts(stream, size, null, latchedListener);

        latch.await();
        assertThat(exceptionHolder.get().getMessage(), containsString("Model size does not match"));
        verify(client, times(totalParts)).execute(eq(PutTrainedModelDefinitionPartAction.INSTANCE), any(), any());
    }

    public void testDigestMismatch() throws InterruptedException {
        var client = mockClient(false);
        var task = mock(ModelDownloadTask.class);
        var config = mock(ModelPackageConfig.class);

        var importer = new ModelImporter(client, "foo", config, task, threadPool);
        int totalParts = 5;
        int chunkSize = 10;
        long size = totalParts * chunkSize;
        var modelDef = modelDefinition(totalParts, chunkSize);
        var stream = mockStreamChunker(modelDef, totalParts, chunkSize);

        when(config.getSha256()).thenReturn("0x"); // digest is different
        when(config.getSize()).thenReturn(size);

        var exceptionHolder = new AtomicReference<Exception>();
        var latch = new CountDownLatch(1);
        var latchedListener = new LatchedActionListener<AcknowledgedResponse>(
            ActionTestUtils.assertNoSuccessListener(exceptionHolder::set),
            latch
        );
        importer.downloadParts(stream, size, null, latchedListener);

        latch.await();
        assertThat(exceptionHolder.get().getMessage(), containsString("Model sha256 checksums do not match"));
        verify(client, times(totalParts)).execute(eq(PutTrainedModelDefinitionPartAction.INSTANCE), any(), any());
    }

    public void testPutFailure() throws InterruptedException {
        var client = mockClient(true);  // client will fail put
        var task = mock(ModelDownloadTask.class);
        var config = mock(ModelPackageConfig.class);

        var importer = new ModelImporter(client, "foo", config, task, threadPool);
        int totalParts = 4;
        int chunkSize = 10;
        var modelDef = modelDefinition(totalParts, chunkSize);
        var stream = mockStreamChunker(modelDef, totalParts, chunkSize);

        var exceptionHolder = new AtomicReference<Exception>();
        var latch = new CountDownLatch(1);
        var latchedListener = new LatchedActionListener<AcknowledgedResponse>(
            ActionTestUtils.assertNoSuccessListener(exceptionHolder::set),
            latch
        );
        importer.downloadParts(stream, totalParts * chunkSize, null, latchedListener);

        latch.await();
        assertThat(exceptionHolder.get().getMessage(), containsString("put model part failed"));
        verify(client, times(1)).execute(eq(PutTrainedModelDefinitionPartAction.INSTANCE), any(), any());
    }

    public void testReadFailure() throws IOException, InterruptedException {
        var client = mockClient(true);
        var task = mock(ModelDownloadTask.class);
        var config = mock(ModelPackageConfig.class);

        var importer = new ModelImporter(client, "foo", config, task, threadPool);
        var stream = mock(ModelLoaderUtils.InputStreamChunker.class);
        when(stream.next()).thenThrow(new IOException("stream failed"));  // fail the read

        when(stream.getTotalParts()).thenReturn(10);
        when(stream.getCurrentPart()).thenReturn(new AtomicInteger());

        var exceptionHolder = new AtomicReference<Exception>();
        var latch = new CountDownLatch(1);
        var latchedListener = new LatchedActionListener<AcknowledgedResponse>(
            ActionTestUtils.assertNoSuccessListener(exceptionHolder::set),
            latch
        );
        importer.downloadParts(stream, 1L, null, latchedListener);

        latch.await();
        assertThat(exceptionHolder.get().getMessage(), containsString("stream failed"));
    }

    @SuppressWarnings("unchecked")
    public void testUploadVocabFailure() throws InterruptedException {
        var client = mock(Client.class);
        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            listener.onFailure(new ElasticsearchStatusException("put vocab failed", RestStatus.BAD_REQUEST));
            return null;
        }).when(client).execute(eq(PutTrainedModelVocabularyAction.INSTANCE), any(), any());

        var task = mock(ModelDownloadTask.class);
        var config = mock(ModelPackageConfig.class);

        var vocab = new ModelLoaderUtils.VocabularyParts(List.of(), List.of(), List.of());

        var importer = new ModelImporter(client, "foo", config, task, threadPool);
        var stream = mock(ModelLoaderUtils.InputStreamChunker.class);
        var exceptionHolder = new AtomicReference<Exception>();
        var latch = new CountDownLatch(1);
        var latchedListener = new LatchedActionListener<AcknowledgedResponse>(
            ActionTestUtils.assertNoSuccessListener(exceptionHolder::set),
            latch
        );
        importer.downloadParts(stream, 1L, vocab, latchedListener);

        latch.await();
        assertThat(exceptionHolder.get().getMessage(), containsString("put vocab failed"));
        verify(client, times(1)).execute(eq(PutTrainedModelVocabularyAction.INSTANCE), any(), any());
        verify(client, never()).execute(eq(PutTrainedModelDefinitionPartAction.INSTANCE), any(), any());
    }

    private ModelLoaderUtils.InputStreamChunker mockStreamChunker(byte[] modelDef, int totalPart, int chunkSize) {
        var modelDefStream = new ByteArrayInputStream(modelDef);
        return new ModelLoaderUtils.InputStreamChunker(modelDefStream, chunkSize, totalPart);
    }

    private byte[] modelDefinition(int totalParts, int chunkSize) {
        var bytes = new byte[totalParts * chunkSize];
        for (int i = 0; i < totalParts; i++) {
            System.arraycopy(randomByteArrayOfLength(chunkSize), 0, bytes, i * chunkSize, chunkSize);
        }
        return bytes;
    }

    private String computeDigest(byte[] modelDef, int totalParts, int chunkSize) {
        var digest = MessageDigests.sha256();
        digest.update(modelDef);
        // for (int i=0; i<totalParts; i++) {
        // digest.update(modelDef, i * totalParts, chunkSize);
        // }
        return MessageDigests.toHexString(digest.digest());
    }

    @SuppressWarnings("unchecked")
    private Client mockClient(boolean failPutPart) {
        var client = mock(Client.class);
        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            if (failPutPart) {
                listener.onFailure(new IllegalStateException("put model part failed"));
            } else {
                listener.onResponse(AcknowledgedResponse.TRUE);
            }
            return null;
        }).when(client).execute(eq(PutTrainedModelDefinitionPartAction.INSTANCE), any(), any());

        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(client).execute(eq(PutTrainedModelVocabularyAction.INSTANCE), any(), any());

        return client;
    }
}
