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
import org.elasticsearch.common.bytes.BytesArray;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
    public void setUp() {
        threadPool = createThreadPool(MachineLearningPackageLoader.modelDownloadExecutor(Settings.EMPTY));
    }

    @After
    public void tearDown() {
        threadPool.close();
    }

    public void testDownload() throws IOException {
        var client = mockClient(false);
        var task = mock(ModelDownloadTask.class);
        var config = mock(ModelPackageConfig.class);
        var vocab = new ModelLoaderUtils.VocabularyParts(List.of(), List.of(), List.of());

        var importer = new ModelImporter(client, "foo", config, task, threadPool);
        int totalParts = 5;
        var modelDef = modelDefinition(totalParts);
        long size = modelDef.stream().mapToInt(BytesArray::length).sum();
        var stream = mockStreamChunker(modelDef);

        var digest = computeDigest(modelDef);
        when(stream.getSha256()).thenReturn(digest);
        when(config.getSha256()).thenReturn(digest);
        when(config.getSize()).thenReturn(size);
        when(stream.getTotalBytesRead()).thenReturn(size);

        importer.downloadParts(stream, size, vocab, ActionListener.wrap(r -> { ; }, ESTestCase::fail));

        verify(client, times(totalParts)).execute(eq(PutTrainedModelDefinitionPartAction.INSTANCE), any(), any());
    }

    public void testSizeMismatch() throws IOException {
        var client = mockClient(false);
        var task = mock(ModelDownloadTask.class);
        var config = mock(ModelPackageConfig.class);

        var importer = new ModelImporter(client, "foo", config, task, threadPool);
        int totalParts = 5;
        var modelDef = modelDefinition(totalParts);
        long size = modelDef.stream().mapToInt(BytesArray::length).sum();
        var stream = mockStreamChunker(modelDef);

        var digest = computeDigest(modelDef);
        when(stream.getSha256()).thenReturn(digest);
        when(config.getSha256()).thenReturn(digest);
        when(config.getSize()).thenReturn(size);
        when(stream.getTotalBytesRead()).thenReturn(size - 1); // expected size and read size are different

        var exceptionHolder = new AtomicReference<Exception>();

        importer.downloadParts(stream, size, null, ActionListener.wrap(ignore -> {}, exceptionHolder::set));

        assertThat(exceptionHolder.get().getMessage(), containsString("Model size does not match"));
        verify(client, times(totalParts)).execute(eq(PutTrainedModelDefinitionPartAction.INSTANCE), any(), any());
    }

    public void testDigestMismatch() throws IOException {
        var client = mockClient(false);
        var task = mock(ModelDownloadTask.class);
        var config = mock(ModelPackageConfig.class);

        var importer = new ModelImporter(client, "foo", config, task, threadPool);
        int totalParts = 5;
        var modelDef = modelDefinition(totalParts);
        long size = modelDef.stream().mapToInt(BytesArray::length).sum();
        var stream = mockStreamChunker(modelDef);

        var digest = computeDigest(modelDef);
        when(stream.getSha256()).thenReturn(digest);
        when(config.getSha256()).thenReturn("0x"); // digest is different
        when(config.getSize()).thenReturn(size);
        when(stream.getTotalBytesRead()).thenReturn(size);

        var exceptionHolder = new AtomicReference<Exception>();

        importer.downloadParts(stream, size, null, ActionListener.wrap(ignore -> {}, exceptionHolder::set));

        assertThat(exceptionHolder.get().getMessage(), containsString("Model sha256 checksums do not match"));
        verify(client, times(totalParts)).execute(eq(PutTrainedModelDefinitionPartAction.INSTANCE), any(), any());
    }

    public void testPutFailure() throws IOException {
        var client = mockClient(true);
        var task = mock(ModelDownloadTask.class);
        var config = mock(ModelPackageConfig.class);

        var importer = new ModelImporter(client, "foo", config, task, threadPool);
        int totalParts = 4;
        var modelDef = modelDefinition(totalParts);
        int size = modelDef.stream().mapToInt(BytesArray::length).sum();
        var stream = mockStreamChunker(modelDef);

        var exceptionHolder = new AtomicReference<Exception>();

        importer.downloadParts(stream, totalParts, null, ActionListener.wrap(r -> fail("unexpected response"), exceptionHolder::set));

        assertThat(exceptionHolder.get().getMessage(), containsString("put model part failed"));
        verify(client, times(1)).execute(eq(PutTrainedModelDefinitionPartAction.INSTANCE), any(), any());
    }

    public void testReadFailure() throws IOException {
        var client = mockClient(true);
        var task = mock(ModelDownloadTask.class);
        var config = mock(ModelPackageConfig.class);

        var importer = new ModelImporter(client, "foo", config, task, threadPool);
        int totalParts = 4;
        var stream = mock(ModelLoaderUtils.InputStreamChunker.class);
        when(stream.next()).thenThrow(new IOException("stream failed"));

        var exceptionHolder = new AtomicReference<Exception>();

        importer.downloadParts(stream, 1L, null, ActionListener.wrap(r -> fail("unexpected response"), exceptionHolder::set));

        assertThat(exceptionHolder.get().getMessage(), containsString("stream failed"));
    }

    @SuppressWarnings("unchecked")
    public void testUploadVocabFailure() {
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

        importer.downloadParts(stream, 1L, vocab, ActionListener.wrap(r -> fail("put vocab failed"), exceptionHolder::set));

        assertThat(exceptionHolder.get().getMessage(), containsString("put vocab failed"));
        verify(client, times(1)).execute(eq(PutTrainedModelVocabularyAction.INSTANCE), any(), any());
        verify(client, never()).execute(eq(PutTrainedModelDefinitionPartAction.INSTANCE), any(), any());
    }

    private ModelLoaderUtils.InputStreamChunker mockStreamChunker(List<BytesArray> modelDef) throws IOException {
        var streamChunker = mock(ModelLoaderUtils.InputStreamChunker.class);

        var first = modelDef.get(0);
        var others = new BytesArray[modelDef.size() - 1];
        for (int i = 0; i < modelDef.size() - 1; i++) {
            others[i] = modelDef.get(i + 1);
        }

        when(streamChunker.next()).thenReturn(first, others);
        return streamChunker;
    }

    private List<BytesArray> modelDefinition(int totalParts) {
        var parts = new ArrayList<BytesArray>();
        for (int i = 0; i < totalParts; i++) {
            parts.add(new BytesArray(randomByteArrayOfLength(4)));
        }
        return parts;
    }

    private String computeDigest(List<BytesArray> parts) {
        var digest = MessageDigests.sha256();
        for (var part : parts) {
            digest.update(part.array());
        }
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
