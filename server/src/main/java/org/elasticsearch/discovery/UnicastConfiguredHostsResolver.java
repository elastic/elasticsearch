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

package org.elasticsearch.discovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.discovery.PeerFinder.ConfiguredHostsResolver;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.discovery.zen.UnicastZenPing;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.discovery.zen.UnicastZenPing.DISCOVERY_ZEN_PING_UNICAST_CONCURRENT_CONNECTS_SETTING;

public class UnicastConfiguredHostsResolver extends AbstractLifecycleComponent implements ConfiguredHostsResolver {
    private static final Logger logger = LogManager.getLogger(UnicastConfiguredHostsResolver.class);

    private final Settings settings;
    private final AtomicBoolean resolveInProgress = new AtomicBoolean();
    private final TransportService transportService;
    private final UnicastHostsProvider hostsProvider;
    private final SetOnce<ExecutorService> executorService = new SetOnce<>();
    private final TimeValue resolveTimeout;
    private final String nodeName;

    public UnicastConfiguredHostsResolver(String nodeName, Settings settings, TransportService transportService,
                                          UnicastHostsProvider hostsProvider) {
        super(settings);
        this.settings = settings;
        this.nodeName = nodeName;
        this.transportService = transportService;
        this.hostsProvider = hostsProvider;
        resolveTimeout = UnicastZenPing.DISCOVERY_ZEN_PING_UNICAST_HOSTS_RESOLVE_TIMEOUT.get(settings);
    }

    @Override
    protected void doStart() {
        final int concurrentConnects = DISCOVERY_ZEN_PING_UNICAST_CONCURRENT_CONNECTS_SETTING.get(settings);
        logger.debug("using concurrent_connects [{}], resolve_timeout [{}]", concurrentConnects, resolveTimeout);
        final ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(settings, "[unicast_configured_hosts_resolver]");
        executorService.set(EsExecutors.newScaling(nodeName + "/" + "unicast_configured_hosts_resolver",
            0, concurrentConnects, 60, TimeUnit.SECONDS, threadFactory, transportService.getThreadPool().getThreadContext()));
    }

    @Override
    protected void doStop() {
        ThreadPool.terminate(executorService.get(), 10, TimeUnit.SECONDS);
    }

    @Override
    protected void doClose() {
    }

    @Override
    public void resolveConfiguredHosts(Consumer<List<TransportAddress>> consumer) {
        if (lifecycle.started() == false) {
            logger.debug("resolveConfiguredHosts: lifecycle is {}, not proceeding", lifecycle);
            return;
        }

        if (resolveInProgress.compareAndSet(false, true)) {
            transportService.getThreadPool().generic().execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.debug("failure when resolving unicast hosts list", e);
                }

                @Override
                protected void doRun() {
                    if (lifecycle.started() == false) {
                        logger.debug("resolveConfiguredHosts.doRun: lifecycle is {}, not proceeding", lifecycle);
                        return;
                    }

                    List<TransportAddress> providedAddresses
                        = hostsProvider.buildDynamicHosts((hosts, limitPortCounts)
                        -> UnicastZenPing.resolveHostsLists(executorService.get(), logger, hosts, limitPortCounts,
                        transportService, resolveTimeout));

                    consumer.accept(providedAddresses);
                }

                @Override
                public void onAfter() {
                    resolveInProgress.set(false);
                }

                @Override
                public String toString() {
                    return "UnicastConfiguredHostsResolver resolving unicast hosts list";
                }
            });
        }
    }
}
