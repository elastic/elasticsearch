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

package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class OutboundHandlerTests extends ESTestCase {

    private TestThreadPool threadPool = new TestThreadPool(getClass().getName());;
    private OutboundHandler handler;
    private FakeTcpChannel fakeTcpChannel;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        TransportLogger transportLogger = new TransportLogger();
        fakeTcpChannel = new FakeTcpChannel();
        handler = new OutboundHandler(threadPool, BigArrays.NON_RECYCLING_INSTANCE, transportLogger);
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testThing() {
        BytesArray bytesArray = new BytesArray("message".getBytes(StandardCharsets.UTF_8));
        AtomicBoolean isDone = new AtomicBoolean(false);
        handler.sendBytes(fakeTcpChannel, bytesArray, ActionListener.wrap(() -> isDone.set(true)));
        

    }
}
