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

package org.elasticsearch.client;

import org.apache.http.Header;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

public abstract class ESRestHighLevelClientTestCase extends ESRestTestCase {

    private static RestHighLevelClient restHighLevelClient;

    @Before
    public void initHighLevelClient() throws IOException {
        super.initClient();
        if (restHighLevelClient == null) {
            restHighLevelClient = new HighLevelClient(client());
        }
    }

    @AfterClass
    public static void cleanupClient() throws IOException {
        restHighLevelClient.close();
        restHighLevelClient = null;
    }

    protected static RestHighLevelClient highLevelClient() {
        return restHighLevelClient;
    }

    /**
     * Executes the provided request using either the sync method or its async variant, both provided as functions
     */
    protected static <Req, Resp> Resp execute(Req request, SyncMethod<Req, Resp> syncMethod,
                                       AsyncMethod<Req, Resp> asyncMethod, Header... headers) throws IOException {
        if (randomBoolean()) {
            return syncMethod.execute(request, headers);
        } else {
            PlainActionFuture<Resp> future = PlainActionFuture.newFuture();
            asyncMethod.execute(request, future, headers);
            return future.actionGet();
        }
    }

    @FunctionalInterface
    protected interface SyncMethod<Request, Response> {
        Response execute(Request request, Header... headers) throws IOException;
    }

    @FunctionalInterface
    protected interface AsyncMethod<Request, Response> {
        void execute(Request request, ActionListener<Response> listener, Header... headers);
    }

    private static class HighLevelClient extends RestHighLevelClient {
        private HighLevelClient(RestClient restClient) {
            super(restClient, (client) -> {}, Collections.emptyList());
        }
    }
}
