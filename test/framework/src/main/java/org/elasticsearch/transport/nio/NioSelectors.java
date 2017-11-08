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
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

public class NioSelectors {

    public static ArrayList<SocketSelector> socketSelectors(Settings settings, Supplier<SocketEventHandler> eventHandlerSupplier,
                                                            int workerCount, String threadNamePrefix) {
        ArrayList<SocketSelector> socketSelectors = new ArrayList<>(workerCount);

        try {
            for (int i = 0; i < workerCount; ++i) {
                SocketSelector selector = new SocketSelector(eventHandlerSupplier.get());
                socketSelectors.add(selector);
            }
        } catch (IOException e) {
            for (SocketSelector selector : socketSelectors) {
                IOUtils.closeWhileHandlingException(selector.rawSelector());
            }
            throw new ElasticsearchException(e);
        }

        for (SocketSelector selector : socketSelectors) {
            if (selector.isRunning() == false) {
                ThreadFactory threadFactory = daemonThreadFactory(settings, threadNamePrefix);
                threadFactory.newThread(selector::runLoop).start();
                selector.isRunningFuture().actionGet();
            }
        }

        return socketSelectors;
    }

    public static ArrayList<AcceptingSelector> acceptingSelectors(Logger logger, Settings settings, OpenChannels openChannels,
                                                                  ArrayList<SocketSelector> socketSelectors, int acceptorCount,
                                                                  String threadNamePrefix) {
        ArrayList<AcceptingSelector> acceptors = new ArrayList<>(acceptorCount);

        try {
            for (int i = 0; i < acceptorCount; ++i) {
                Supplier<SocketSelector> selectorSupplier = new RoundRobinSelectorSupplier(socketSelectors);
                AcceptorEventHandler eventHandler = new AcceptorEventHandler(logger, openChannels, selectorSupplier);
                AcceptingSelector acceptor = new AcceptingSelector(eventHandler);
                acceptors.add(acceptor);
            }
        } catch (IOException e) {
            for (AcceptingSelector selector : acceptors) {
                IOUtils.closeWhileHandlingException(selector.rawSelector());
            }
            throw new ElasticsearchException(e);
        }

        for (AcceptingSelector acceptor : acceptors) {
            if (acceptor.isRunning() == false) {
                ThreadFactory threadFactory = daemonThreadFactory(settings, threadNamePrefix);
                threadFactory.newThread(acceptor::runLoop).start();
                acceptor.isRunningFuture().actionGet();
            }
        }

        return acceptors;
    }
}
