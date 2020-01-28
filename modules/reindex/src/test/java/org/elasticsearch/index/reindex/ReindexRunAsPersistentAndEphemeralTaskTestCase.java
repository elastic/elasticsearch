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

package org.elasticsearch.index.reindex;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This abstract test case will run all the same tests, both as a persistent reindex task (resilient) and as an old style ephemeral task
 * (non-resilient).
 * This should eventually go away (only necessary in 7.x).
 */
public abstract class ReindexRunAsPersistentAndEphemeralTaskTestCase extends ReindexTestCase {
    private final String name;
    private ReindexRequestBuilderFactory requestBuilderFactory;

    private static final Map<String, ReindexRequestBuilderFactory> requestBuilderFactories = new LinkedHashMap<>();
    static {
        requestBuilderFactories.put("ephemeral task", client -> new ReindexRequestBuilder(client, ReindexAction.INSTANCE));
        requestBuilderFactories.put("non-resilient persistent task",
            client -> new ReindexRequestBuilder(client, ReindexAction.INSTANCE) {
                @Override
                public ActionFuture<BulkByScrollResponse> execute() {
                    PlainActionFuture<BulkByScrollResponse> futureResult = new PlainActionFuture<>();
                    client.execute(StartReindexTaskAction.INSTANCE, new StartReindexTaskAction.Request(request(), true, false),
                        ActionListener.delegateFailure(futureResult, (future, result) -> future.onResponse(result.getReindexResponse())));

                    return futureResult;
                }
            });
        requestBuilderFactories.put("resilient persistent task",
            client -> new ReindexRequestBuilder(client, ReindexAction.INSTANCE) {
                @Override
                public ActionFuture<BulkByScrollResponse> execute() {
                    PlainActionFuture<BulkByScrollResponse> futureResult = new PlainActionFuture<>();
                    client.execute(StartReindexTaskAction.INSTANCE, new StartReindexTaskAction.Request(request(), true, true),
                        ActionListener.delegateFailure(futureResult, (future, result) -> future.onResponse(result.getReindexResponse())));

                    return futureResult;
                }
            });
    }

    @ParametersFactory
    public static Iterable<Object[]> requestFactoryParameters() {
        return requestBuilderFactories.keySet().stream().map(name -> new Object[]{name}).collect(Collectors.toList());
    }

    protected interface ReindexRequestBuilderFactory {
        ReindexRequestBuilder create(Client client);
    }

    protected ReindexRunAsPersistentAndEphemeralTaskTestCase(String name) {
        this.name = name;
        this.requestBuilderFactory = requestBuilderFactories.get(name);
    }

    @Override
    protected ReindexRequestBuilder reindex(Client client) {
        return requestBuilderFactory.create(client);
    }

    protected void assumeTaskTest() {
        assumeTrue("does not support resilient job", "task".equals(name));
    }
}
