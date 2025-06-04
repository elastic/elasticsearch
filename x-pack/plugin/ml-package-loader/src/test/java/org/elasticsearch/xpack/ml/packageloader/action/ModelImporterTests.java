/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.packageloader.action;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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

    public void testDownloadModelDefinition() throws InterruptedException, URISyntaxException {
        var client = mockClient(false);
        var task = ModelDownloadTaskTests.testTask();
        var config = mockConfigWithRepoLinks();
        var vocab = new ModelLoaderUtils.VocabularyParts(List.of(), List.of(), List.of());
        var cbs = mock(CircuitBreakerService.class);
        when(cbs.getBreaker(eq(CircuitBreaker.REQUEST))).thenReturn(mock(CircuitBreaker.class));

        int totalParts = 5;
        int chunkSize = 10;
        long size = totalParts * chunkSize;
        var modelDef = modelDefinition(totalParts, chunkSize);
        var streamers = mockHttpStreamChunkers(modelDef, chunkSize, 2);

        var digest = computeDigest(modelDef);
        when(config.getSha256()).thenReturn(digest);
        when(config.getSize()).thenReturn(size);

        var importer = new ModelImporter(client, "foo", config, task, threadPool, cbs);

        var latch = new CountDownLatch(1);
        var latchedListener = new LatchedActionListener<AcknowledgedResponse>(ActionTestUtils.assertNoFailureListener(ignore -> {}), latch);
        importer.downloadModelDefinition(size, totalParts, vocab, streamers, latchedListener);

        latch.await();
        verify(client, times(totalParts)).execute(eq(PutTrainedModelDefinitionPartAction.INSTANCE), any());
        assertEquals(totalParts - 1, task.getStatus().downloadProgress().downloadedParts());
        assertEquals(totalParts, task.getStatus().downloadProgress().totalParts());
    }

    public void testReadModelDefinitionFromFile() throws InterruptedException, URISyntaxException {
        var client = mockClient(false);
        var task = ModelDownloadTaskTests.testTask();
        var config = mockConfigWithRepoLinks();
        var vocab = new ModelLoaderUtils.VocabularyParts(List.of(), List.of(), List.of());
        var cbs = mock(CircuitBreakerService.class);
        when(cbs.getBreaker(eq(CircuitBreaker.REQUEST))).thenReturn(mock(CircuitBreaker.class));

        int totalParts = 3;
        int chunkSize = 10;
        long size = totalParts * chunkSize;
        var modelDef = modelDefinition(totalParts, chunkSize);

        var digest = computeDigest(modelDef);
        when(config.getSha256()).thenReturn(digest);
        when(config.getSize()).thenReturn(size);

        var importer = new ModelImporter(client, "foo", config, task, threadPool, cbs);
        var streamChunker = new ModelLoaderUtils.InputStreamChunker(new ByteArrayInputStream(modelDef), chunkSize);

        var latch = new CountDownLatch(1);
        var latchedListener = new LatchedActionListener<AcknowledgedResponse>(ActionTestUtils.assertNoFailureListener(ignore -> {}), latch);
        importer.readModelDefinitionFromFile(size, totalParts, streamChunker, vocab, latchedListener);

        latch.await();
        verify(client, times(totalParts)).execute(eq(PutTrainedModelDefinitionPartAction.INSTANCE), any());
        assertEquals(totalParts, task.getStatus().downloadProgress().downloadedParts());
        assertEquals(totalParts, task.getStatus().downloadProgress().totalParts());
    }

    public void testSizeMismatch() throws InterruptedException, URISyntaxException {
        var client = mockClient(false);
        var task = mock(ModelDownloadTask.class);
        var config = mockConfigWithRepoLinks();
        var cbs = mock(CircuitBreakerService.class);
        when(cbs.getBreaker(eq(CircuitBreaker.REQUEST))).thenReturn(mock(CircuitBreaker.class));

        int totalParts = 5;
        int chunkSize = 10;
        long size = totalParts * chunkSize;
        var modelDef = modelDefinition(totalParts, chunkSize);
        var streamers = mockHttpStreamChunkers(modelDef, chunkSize, 2);

        var digest = computeDigest(modelDef);
        when(config.getSha256()).thenReturn(digest);
        when(config.getSize()).thenReturn(size - 1); // expected size and read size are different

        var exceptionHolder = new AtomicReference<Exception>();

        var latch = new CountDownLatch(1);
        var latchedListener = new LatchedActionListener<AcknowledgedResponse>(
            ActionTestUtils.assertNoSuccessListener(exceptionHolder::set),
            latch
        );

        var importer = new ModelImporter(client, "foo", config, task, threadPool, cbs);
        importer.downloadModelDefinition(size, totalParts, null, streamers, latchedListener);

        latch.await();
        assertThat(exceptionHolder.get().getMessage(), containsString("Model size does not match"));
        verify(client, times(totalParts)).execute(eq(PutTrainedModelDefinitionPartAction.INSTANCE), any());
    }

    public void testDigestMismatch() throws InterruptedException, URISyntaxException {
        var client = mockClient(false);
        var task = mock(ModelDownloadTask.class);
        var config = mockConfigWithRepoLinks();
        var cbs = mock(CircuitBreakerService.class);
        when(cbs.getBreaker(eq(CircuitBreaker.REQUEST))).thenReturn(mock(CircuitBreaker.class));

        int totalParts = 5;
        int chunkSize = 10;
        long size = totalParts * chunkSize;
        var modelDef = modelDefinition(totalParts, chunkSize);
        var streamers = mockHttpStreamChunkers(modelDef, chunkSize, 2);

        when(config.getSha256()).thenReturn("0x"); // digest is different
        when(config.getSize()).thenReturn(size);

        var exceptionHolder = new AtomicReference<Exception>();
        var latch = new CountDownLatch(1);
        var latchedListener = new LatchedActionListener<AcknowledgedResponse>(
            ActionTestUtils.assertNoSuccessListener(exceptionHolder::set),
            latch
        );

        var importer = new ModelImporter(client, "foo", config, task, threadPool, cbs);
        // Message digest can only be calculated for the file reader
        var streamChunker = new ModelLoaderUtils.InputStreamChunker(new ByteArrayInputStream(modelDef), chunkSize);
        importer.readModelDefinitionFromFile(size, totalParts, streamChunker, null, latchedListener);

        latch.await();
        assertThat(exceptionHolder.get().getMessage(), containsString("Model sha256 checksums do not match"));
        verify(client, times(totalParts)).execute(eq(PutTrainedModelDefinitionPartAction.INSTANCE), any());
    }

    public void testPutFailure() throws InterruptedException, URISyntaxException {
        var client = mockClient(true);  // client will fail put
        var task = mock(ModelDownloadTask.class);
        var config = mockConfigWithRepoLinks();
        var cbs = mock(CircuitBreakerService.class);
        when(cbs.getBreaker(eq(CircuitBreaker.REQUEST))).thenReturn(mock(CircuitBreaker.class));

        int totalParts = 4;
        int chunkSize = 10;
        long size = totalParts * chunkSize;
        var modelDef = modelDefinition(totalParts, chunkSize);
        var streamers = mockHttpStreamChunkers(modelDef, chunkSize, 1);

        var exceptionHolder = new AtomicReference<Exception>();
        var latch = new CountDownLatch(1);
        var latchedListener = new LatchedActionListener<AcknowledgedResponse>(
            ActionTestUtils.assertNoSuccessListener(exceptionHolder::set),
            latch
        );

        var importer = new ModelImporter(client, "foo", config, task, threadPool, cbs);
        importer.downloadModelDefinition(size, totalParts, null, streamers, latchedListener);

        latch.await();
        assertThat(exceptionHolder.get().getMessage(), containsString("put model part failed"));
        verify(client, times(1)).execute(eq(PutTrainedModelDefinitionPartAction.INSTANCE), any());
    }

    public void testReadFailure() throws IOException, InterruptedException, URISyntaxException {
        var client = mockClient(true);
        var task = mock(ModelDownloadTask.class);
        var config = mockConfigWithRepoLinks();
        var cbs = mock(CircuitBreakerService.class);
        when(cbs.getBreaker(eq(CircuitBreaker.REQUEST))).thenReturn(mock(CircuitBreaker.class));

        int totalParts = 4;
        int chunkSize = 10;
        long size = totalParts * chunkSize;

        var streamer = mock(ModelLoaderUtils.HttpStreamChunker.class);
        when(streamer.hasNext()).thenReturn(true);
        when(streamer.next()).thenThrow(new IOException("stream failed"));  // fail the read

        var exceptionHolder = new AtomicReference<Exception>();
        var latch = new CountDownLatch(1);
        var latchedListener = new LatchedActionListener<AcknowledgedResponse>(
            ActionTestUtils.assertNoSuccessListener(exceptionHolder::set),
            latch
        );

        var importer = new ModelImporter(client, "foo", config, task, threadPool, cbs);
        importer.downloadModelDefinition(size, totalParts, null, List.of(streamer), latchedListener);

        latch.await();
        assertThat(exceptionHolder.get().getMessage(), containsString("stream failed"));
    }

    @SuppressWarnings("unchecked")
    public void testUploadVocabFailure() throws InterruptedException, URISyntaxException {
        var client = mock(Client.class);
        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            listener.onFailure(new ElasticsearchStatusException("put vocab failed", RestStatus.BAD_REQUEST));
            return null;
        }).when(client).execute(eq(PutTrainedModelVocabularyAction.INSTANCE), any(), any());
        var cbs = mock(CircuitBreakerService.class);
        when(cbs.getBreaker(eq(CircuitBreaker.REQUEST))).thenReturn(mock(CircuitBreaker.class));

        var task = mock(ModelDownloadTask.class);
        var config = mockConfigWithRepoLinks();

        var vocab = new ModelLoaderUtils.VocabularyParts(List.of(), List.of(), List.of());

        var exceptionHolder = new AtomicReference<Exception>();
        var latch = new CountDownLatch(1);
        var latchedListener = new LatchedActionListener<AcknowledgedResponse>(
            ActionTestUtils.assertNoSuccessListener(exceptionHolder::set),
            latch
        );

        var importer = new ModelImporter(client, "foo", config, task, threadPool, cbs);
        importer.downloadModelDefinition(100, 5, vocab, List.of(), latchedListener);

        latch.await();
        assertThat(exceptionHolder.get().getMessage(), containsString("put vocab failed"));
        verify(client, times(1)).execute(eq(PutTrainedModelVocabularyAction.INSTANCE), any(), any());
        verify(client, never()).execute(eq(PutTrainedModelDefinitionPartAction.INSTANCE), any());
    }

    private List<ModelLoaderUtils.HttpStreamChunker> mockHttpStreamChunkers(byte[] modelDef, int chunkSize, int numStreams) {
        var ranges = ModelLoaderUtils.split(modelDef.length, numStreams, chunkSize);

        var result = new ArrayList<ModelLoaderUtils.HttpStreamChunker>(ranges.size());
        for (var range : ranges) {
            int len = range.numParts() * chunkSize;
            var modelDefStream = new ByteArrayInputStream(modelDef, (int) range.rangeStart(), len);
            result.add(new ModelLoaderUtils.HttpStreamChunker(modelDefStream, range, chunkSize));
        }

        return result;
    }

    private byte[] modelDefinition(int totalParts, int chunkSize) {
        var bytes = new byte[totalParts * chunkSize];
        for (int i = 0; i < totalParts; i++) {
            System.arraycopy(randomByteArrayOfLength(chunkSize), 0, bytes, i * chunkSize, chunkSize);
        }
        return bytes;
    }

    private String computeDigest(byte[] modelDef) {
        var digest = MessageDigests.sha256();
        digest.update(modelDef);
        return MessageDigests.toHexString(digest.digest());
    }

    @SuppressWarnings("unchecked")
    private Client mockClient(boolean failPutPart) {
        var client = mock(Client.class);

        if (failPutPart) {
            when(client.execute(eq(PutTrainedModelDefinitionPartAction.INSTANCE), any())).thenThrow(
                new IllegalStateException("put model part failed")
            );
        } else {
            ActionFuture<AcknowledgedResponse> future = mock(ActionFuture.class);
            when(future.actionGet()).thenReturn(AcknowledgedResponse.TRUE);
            when(client.execute(eq(PutTrainedModelDefinitionPartAction.INSTANCE), any())).thenReturn(future);
        }

        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(client).execute(eq(PutTrainedModelVocabularyAction.INSTANCE), any(), any());

        return client;
    }

    private ModelPackageConfig mockConfigWithRepoLinks() {
        var config = mock(ModelPackageConfig.class);
        when(config.getModelRepository()).thenReturn("https://models.models");
        when(config.getPackagedModelId()).thenReturn("my-model");
        return config;
    }
}
