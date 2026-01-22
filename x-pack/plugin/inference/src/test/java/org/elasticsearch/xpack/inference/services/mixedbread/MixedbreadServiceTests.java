/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.InferenceServiceTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.mock;

public class MixedbreadServiceTests extends InferenceServiceTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityExecutors());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @After
    public void shutdown() throws IOException {
        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    public void testGetConfiguration() throws Exception {
        try (var service = createMixedbreadService()) {
            String content = XContentHelper.stripWhitespace("""
                {
                        "service": "mixedbread",
                        "name": "Mixedbread",
                        "task_types": ["rerank"],
                        "configurations": {
                            "api_key": {
                                "description": "API Key for the provider you're connecting to.",
                                "label": "API Key",
                                "required": true,
                                "sensitive": true,
                                "updatable": true,
                                "type": "str",
                                "supported_task_types": ["rerank"]
                            },
                            "model_id": {
                                "description": "The name of the model to use for the inference task.",
                                "label": "Model ID",
                                "required": true,
                                "sensitive": false,
                                "updatable": false,
                                "type": "str",
                                "supported_task_types": ["rerank"]
                            },
                            "rate_limit.requests_per_minute": {
                                "description": "Minimize the number of rate limit errors.",
                                "label": "Rate Limit",
                                "required": false,
                                "sensitive": false,
                                "updatable": false,
                                "type": "int",
                                "supported_task_types": ["rerank"]
                            }
                        }
                    }
                """);
            InferenceServiceConfiguration configuration = InferenceServiceConfiguration.fromXContentBytes(
                new BytesArray(content),
                XContentType.JSON
            );
            boolean humanReadable = true;
            BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
            InferenceServiceConfiguration serviceConfiguration = service.getConfiguration();
            assertToXContentEquivalent(
                originalBytes,
                toXContent(serviceConfiguration, XContentType.JSON, humanReadable),
                XContentType.JSON
            );
        }
    }

    private MixedbreadService createMixedbreadService() {
        return new MixedbreadService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool), mockClusterServiceEmpty());
    }

    @Override
    public InferenceService createInferenceService() {
        return createMixedbreadService();
    }

    @Override
    protected void assertRerankerWindowSize(RerankingInferenceService rerankingInferenceService) {
        assertThat(rerankingInferenceService.rerankerWindowSize("any model"), is(8000));
    }
}
