/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.client;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;

/**
 * Client that always responds with {@code null} to every request. Override this for testing.
 */
public class NoOpClient extends AbstractClient {
    /**
     * Build with {@link ThreadPool}. This {@linkplain ThreadPool} is terminated on {@link #close()}.
     */
    public NoOpClient(ThreadPool threadPool) {
        super(Settings.EMPTY, threadPool);
    }

    /**
     * Create a new {@link TestThreadPool} for this client.
     */
    public NoOpClient(String testName) {
        super(Settings.EMPTY, new TestThreadPool(testName));
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse>
    void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
        listener.onResponse(null);
    }

    @Override
    public void close() {
        try {
            ThreadPool.terminate(threadPool(), 10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new ElasticsearchException(e.getMessage(), e);
        }
    }
}
