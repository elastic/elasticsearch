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

package org.elasticsearch.transport.nio;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.nio.EventHandler;
import org.elasticsearch.nio.NioGroup;
import org.elasticsearch.nio.NioSelector;
import org.elasticsearch.transport.TcpTransport;

import java.io.IOException;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

public class NioGroupFactory {

    private final Logger logger;
    private final Settings settings;
    private final int httpWorkerCount;

    private volatile RefNioGroup nioGroup;

    public NioGroupFactory(Settings settings, Logger logger) {
        this.logger = logger;
        this.settings = settings;
        this.httpWorkerCount = NioTransportPlugin.NIO_HTTP_WORKER_COUNT.get(settings);
    }

    public synchronized NioGroup getTransportGroup() throws IOException {
        return getGenericGroup();
    }

    public synchronized NioGroup getHttpGroup() throws IOException {
        if (httpWorkerCount == 0) {
            return getGenericGroup();
        } else {
            return new NioGroup(daemonThreadFactory(this.settings, HttpServerTransport.HTTP_SERVER_WORKER_THREAD_NAME_PREFIX),
                httpWorkerCount, (s) -> new EventHandler(this::onException, s));
        }
    }

    private NioGroup getGenericGroup() throws IOException {
        if (nioGroup == null) {
            nioGroup = new RefNioGroup(daemonThreadFactory(this.settings, TcpTransport.TRANSPORT_WORKER_THREAD_NAME_PREFIX),
                NioTransportPlugin.NIO_WORKER_COUNT.get(settings), (s) -> new EventHandler(this::onException, s));
        }
        nioGroup.incReference();
        return nioGroup;
    }

    private void onException(Exception exception) {
        logger.warn(new ParameterizedMessage("exception caught on transport layer [thread={}]", Thread.currentThread().getName()),
            exception);
    }

    private static class RefNioGroup extends NioGroup {

        private int references = 0;

        private RefNioGroup(ThreadFactory threadFactory, int selectorCount,
                            Function<Supplier<NioSelector>, EventHandler> eventHandlerFunction) throws IOException {
            super(threadFactory, selectorCount, eventHandlerFunction);
        }

        private synchronized void incReference() {
            references++;
        }

        @Override
        public synchronized void close() throws IOException {
            if (references-- == 0) {
                super.close();
            }
        }
    }
}
