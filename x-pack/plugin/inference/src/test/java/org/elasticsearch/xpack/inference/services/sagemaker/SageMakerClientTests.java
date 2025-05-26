/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker;

import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.sagemakerruntime.SageMakerRuntimeAsyncClient;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointRequest;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointResponse;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointWithResponseStreamRequest;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointWithResponseStreamResponse;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointWithResponseStreamResponseHandler;
import software.amazon.awssdk.services.sagemakerruntime.model.ResponseStream;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.amazon.AwsSecretSettings;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentMatchers;
import org.reactivestreams.Subscriber;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SageMakerClientTests extends ESTestCase {
    private static final InvokeEndpointRequest REQUEST = InvokeEndpointRequest.builder().build();
    private static final InvokeEndpointWithResponseStreamRequest STREAM_REQUEST = InvokeEndpointWithResponseStreamRequest.builder().build();
    private SageMakerRuntimeAsyncClient awsClient;
    private CacheLoader<SageMakerClient.RegionAndSecrets, SageMakerRuntimeAsyncClient> clientFactory;
    private SageMakerClient client;
    private ThreadPool threadPool;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = createThreadPool(inferenceUtilityPool());

        awsClient = mock();
        clientFactory = spy(new CacheLoader<>() {
            public SageMakerRuntimeAsyncClient load(SageMakerClient.RegionAndSecrets key) {
                return awsClient;
            }
        });
        client = new SageMakerClient(clientFactory, threadPool);
    }

    @After
    public void shutdown() throws IOException {
        terminate(threadPool);
    }

    public void testInvoke() throws Exception {
        var expectedResponse = InvokeEndpointResponse.builder().build();
        when(awsClient.invokeEndpoint(any(InvokeEndpointRequest.class))).thenReturn(CompletableFuture.completedFuture(expectedResponse));

        var listener = invoke(TimeValue.THIRTY_SECONDS);
        verify(clientFactory, times(1)).load(any());
        verify(listener, times(1)).onResponse(eq(expectedResponse));
    }

    private static SageMakerClient.RegionAndSecrets regionAndSecrets() {
        return new SageMakerClient.RegionAndSecrets(
            "us-east-1",
            new AwsSecretSettings(new SecureString("access"), new SecureString("secrets"))
        );
    }

    private ActionListener<InvokeEndpointResponse> invoke(TimeValue timeout) throws InterruptedException {
        var latch = new CountDownLatch(1);
        ActionListener<InvokeEndpointResponse> listener = spy(ActionListener.noop());
        client.invoke(regionAndSecrets(), REQUEST, timeout, ActionListener.runAfter(listener, latch::countDown));
        assertTrue("Timed out waiting for invoke call", latch.await(5, TimeUnit.SECONDS));
        return listener;
    }

    public void testInvokeCache() throws Exception {
        when(awsClient.invokeEndpoint(any(InvokeEndpointRequest.class))).thenReturn(
            CompletableFuture.completedFuture(InvokeEndpointResponse.builder().build())
        );

        invoke(TimeValue.THIRTY_SECONDS);
        invoke(TimeValue.THIRTY_SECONDS);
        verify(clientFactory, times(1)).load(any());
    }

    public void testInvokeTimeout() throws Exception {
        when(awsClient.invokeEndpoint(any(InvokeEndpointRequest.class))).thenReturn(new CompletableFuture<>());

        var listener = invoke(TimeValue.timeValueMillis(10));

        verify(clientFactory, times(1)).load(any());
        verifyTimeout(listener);
    }

    private static void verifyTimeout(ActionListener<?> listener) {
        verify(listener, times(1)).onFailure(assertArg(e -> assertThat(e.getMessage(), equalTo("Request timed out after [10ms]"))));
    }

    public void testInvokeStream() throws Exception {
        SdkPublisher<ResponseStream> publisher = mockPublisher();

        var listener = invokeStream(TimeValue.THIRTY_SECONDS);

        verify(publisher, never()).subscribe(ArgumentMatchers.<Subscriber<ResponseStream>>any());
        verify(listener, times(1)).onResponse(assertArg(stream -> stream.responseStream().subscribe(mock())));
        verify(publisher, times(1)).subscribe(ArgumentMatchers.<Subscriber<ResponseStream>>any());
    }

    private SdkPublisher<ResponseStream> mockPublisher() {
        SdkPublisher<ResponseStream> publisher = mock();
        doAnswer(ans -> {
            InvokeEndpointWithResponseStreamResponseHandler handler = ans.getArgument(1);
            handler.responseReceived(InvokeEndpointWithResponseStreamResponse.builder().build());
            handler.onEventStream(publisher);
            return CompletableFuture.completedFuture((Void) null);
        }).when(awsClient).invokeEndpointWithResponseStream(any(InvokeEndpointWithResponseStreamRequest.class), any());
        return publisher;
    }

    private ActionListener<SageMakerClient.SageMakerStream> invokeStream(TimeValue timeout) throws Exception {
        var latch = new CountDownLatch(1);
        ActionListener<SageMakerClient.SageMakerStream> listener = spy(ActionListener.noop());
        client.invokeStream(regionAndSecrets(), STREAM_REQUEST, timeout, ActionListener.runAfter(listener, latch::countDown));
        assertTrue("Timed out waiting for invoke call", latch.await(5, TimeUnit.SECONDS));
        return listener;
    }

    public void testInvokeStreamCache() throws Exception {
        mockPublisher();

        invokeStream(TimeValue.THIRTY_SECONDS);
        invokeStream(TimeValue.THIRTY_SECONDS);

        verify(clientFactory, times(1)).load(any());
    }

    public void testInvokeStreamTimeout() throws Exception {
        when(awsClient.invokeEndpointWithResponseStream(any(InvokeEndpointWithResponseStreamRequest.class), any())).thenReturn(
            new CompletableFuture<>()
        );

        var listener = invokeStream(TimeValue.timeValueMillis(10));

        verify(clientFactory, times(1)).load(any());
        verifyTimeout(listener);
    }

    public void testClose() throws Exception {
        // load cache
        mockPublisher();
        invokeStream(TimeValue.THIRTY_SECONDS);
        // clear cache
        client.close();
        verify(awsClient, times(1)).close();
    }
}
