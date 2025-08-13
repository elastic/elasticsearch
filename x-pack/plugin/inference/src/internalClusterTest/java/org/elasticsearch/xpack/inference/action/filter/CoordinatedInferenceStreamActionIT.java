/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.DeleteInferenceEndpointAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.action.CoordinatedInferenceStreamAction;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.mock.TestStreamingCompletionServiceExtension;
import org.hamcrest.Matchers;
import org.junit.AfterClass;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@ESTestCase.WithoutEntitlements // due to dependency issue ES-12435
@ESIntegTestCase.SuiteScopeTestCase
public class CoordinatedInferenceStreamActionIT extends ESIntegTestCase {
    private static final String inferenceId = "test-streaming-id";

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        super.setupSuiteScopeCluster();
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("service", "streaming_completion_test_service");
            builder.field("service_settings", Map.ofEntries(Map.entry("model", "my_model"), Map.entry("api_key", "abc123")));
            builder.endObject();

            var fakeContent = BytesReference.bytes(builder);
            var request = new PutInferenceModelAction.Request(
                TaskType.COMPLETION,
                inferenceId,
                fakeContent,
                XContentType.JSON,
                TEST_REQUEST_TIMEOUT
            );
            client().execute(PutInferenceModelAction.INSTANCE, request).actionGet(TEST_REQUEST_TIMEOUT);
        }
    }

    @AfterClass
    public static void tearDownInferenceEndpoint() {
        client().execute(
            DeleteInferenceEndpointAction.INSTANCE,
            new DeleteInferenceEndpointAction.Request(inferenceId, TaskType.COMPLETION, true, false)
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(otherSettings).put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class, TestInferenceServicePlugin.class);
    }

    public void testStreaming() throws IOException, InterruptedException {
        var responsePublisher = call();

        var latch = new CountDownLatch(1);
        Stream.Builder<String> results = Stream.builder();

        responsePublisher.subscribe(new Flow.Subscriber<>() {
            private volatile Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                this.subscription = subscription;
                this.subscription.request(1);
            }

            @Override
            public void onNext(InferenceServiceResults.Result item) {
                assert this.subscription != null;
                if (item instanceof TestStreamingCompletionServiceExtension.TestCompletionChunk chunk) {
                    results.add(chunk.delta());
                    this.subscription.request(1);
                } else {
                    fail("Unknown item " + (item != null ? item.getClass().getName() : "null"));
                }
            }

            @Override
            public void onError(Throwable throwable) {
                latch.countDown();
                fail(throwable);
            }

            @Override
            public void onComplete() {
                latch.countDown();
            }
        });

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        assertThat(results.build().toList(), Matchers.containsInRelativeOrder("HELLO", "WORLD"));
    }

    public void testCancel() throws IOException, InterruptedException {
        var responsePublisher = call();

        var latch = new CountDownLatch(1);
        Stream.Builder<String> results = Stream.builder();

        responsePublisher.subscribe(new Flow.Subscriber<>() {
            private volatile Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                this.subscription = subscription;
                this.subscription.request(1);
            }

            @Override
            public void onNext(InferenceServiceResults.Result item) {
                assert this.subscription != null;
                if (item instanceof TestStreamingCompletionServiceExtension.TestCompletionChunk chunk) {
                    results.add(chunk.delta());
                    this.subscription.cancel();
                } else {
                    fail("Unknown item " + (item != null ? item.getClass().getName() : "null"));
                }
            }

            @Override
            public void onError(Throwable throwable) {
                latch.countDown();
                fail(throwable);
            }

            @Override
            public void onComplete() {
                latch.countDown();
            }
        });

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        assertThat(results.build().toList(), Matchers.containsInRelativeOrder("HELLO"));
    }

    private Flow.Publisher<InferenceServiceResults.Result> call() {
        var delegateRequest = new InferenceAction.Request(
            TaskType.COMPLETION,
            inferenceId,
            null,
            null,
            null,
            List.of("hello", "world"),
            null,
            InputType.UNSPECIFIED,
            TEST_REQUEST_TIMEOUT,
            true
        );
        var request = new CoordinatedInferenceStreamAction.Request(delegateRequest, randomTargetNode(), TEST_REQUEST_TIMEOUT);
        var future = client().execute(CoordinatedInferenceStreamAction.INSTANCE, request);
        return future.actionGet(TEST_REQUEST_TIMEOUT).publisher();
    }

    private DiscoveryNode randomTargetNode() {
        return randomFrom(
            internalCluster().client().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState().nodes().getAllNodes()
        );
    }
}
