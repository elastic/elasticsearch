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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.util.concurrent.AbstractLifecycleRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class TcpTransportKeepAlive {

    private final Logger logger = LogManager.getLogger(ConnectionManager.class);
    private final ConcurrentMap<Integer, ScheduledPing> pingIntervals = ConcurrentCollections.newConcurrentMap();
    private final ConcurrentSkipListMap<Long, TcpTransport.NodeChannels> map = null;
    private final Lifecycle lifecycle = new Lifecycle();
    private final ThreadPool threadPool;

    public TcpTransportKeepAlive(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    public void receiveKeepAlive(TcpChannel tcpChannel) {
        boolean isClient = true;
        if (isClient)  {

        } else {
//            tcpChannel.sendMessage(null, null);
        }
    }

    private class KeepAliveChannel {

        private final TcpChannel channel;

        private KeepAliveChannel(TcpChannel channel) {
            this.channel = channel;
        }
    }

    private class ScheduledPing extends AbstractLifecycleRunnable {

        private Set<TcpTransport.NodeChannels> nodes = ConcurrentCollections.newConcurrentSet();

        private ScheduledPing() {
            super(lifecycle, logger);
        }

        @Override
        protected void doRunInLifecycle() {
            for (TcpTransport.NodeChannels nodeChannels : nodes) {
                if (nodeChannels.sendPing() == false) {
                    logger.warn("attempted to send ping to connection without support for pings [{}]", nodeChannels);
                }
            }
        }

        @Override
        protected void onAfterInLifecycle() {
            try {
//                threadPool.schedule(pingSchedule, ThreadPool.Names.GENERIC, this);
            } catch (EsRejectedExecutionException ex) {
                if (ex.isExecutorShutdown()) {
                    logger.debug("couldn't schedule new ping execution, executor is shutting down", ex);
                } else {
                    throw ex;
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (lifecycle.stoppedOrClosed()) {
                logger.trace("failed to send ping transport message", e);
            } else {
                logger.warn("failed to send ping transport message", e);
            }
        }
    }
}
