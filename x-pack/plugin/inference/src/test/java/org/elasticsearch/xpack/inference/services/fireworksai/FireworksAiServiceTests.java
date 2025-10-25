/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;

import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class FireworksAiServiceTests extends ESTestCase {

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testRerankerWindowSize() {
        var service = createFireworksAiService();
        assertThat(service.rerankerWindowSize("any-model"), is(5500));
    }

    public void testServiceName() {
        var service = createFireworksAiService();
        assertThat(service.name(), is("fireworksai"));
    }

    private FireworksAiService createFireworksAiService() {
        return new FireworksAiService(
            mock(HttpRequestSender.Factory.class),
            createWithEmptySettings(threadPool),
            mockClusterServiceEmpty()
        );
    }
}
