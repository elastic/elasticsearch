/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class SeedHostsResolver extends AbstractLifecycleComponent implements ConfiguredHostsResolver {
    public static final Setting<Integer> LEGACY_DISCOVERY_ZEN_PING_UNICAST_CONCURRENT_CONNECTS_SETTING = Setting.intSetting(
        "discovery.zen.ping.unicast.concurrent_connects",
        10,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );
    public static final Setting<TimeValue> LEGACY_DISCOVERY_ZEN_PING_UNICAST_HOSTS_RESOLVE_TIMEOUT = Setting.positiveTimeSetting(
        "discovery.zen.ping.unicast.hosts.resolve_timeout",
        TimeValue.timeValueSeconds(5),
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );
    public static final Setting<Integer> DISCOVERY_SEED_RESOLVER_MAX_CONCURRENT_RESOLVERS_SETTING = Setting.intSetting(
        "discovery.seed_resolver.max_concurrent_resolvers",
        10,
        0,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> DISCOVERY_SEED_RESOLVER_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "discovery.seed_resolver.timeout",
        TimeValue.timeValueSeconds(5),
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(SeedHostsResolver.class);

    private final Settings settings;
    private final AtomicBoolean resolveInProgress = new AtomicBoolean();
    private final TransportService transportService;
    private final SeedHostsProvider hostsProvider;
    private final SetOnce<ExecutorService> executorService = new SetOnce<>();
    private final TimeValue resolveTimeout;
    private final String nodeName;
    private final int concurrentConnects;
    private final CancellableThreads cancellableThreads = new CancellableThreads();

    public SeedHostsResolver(String nodeName, Settings settings, TransportService transportService, SeedHostsProvider seedProvider) {
        this.settings = settings;
        this.nodeName = nodeName;
        this.transportService = transportService;
        this.hostsProvider = seedProvider;
        resolveTimeout = getResolveTimeout(settings);
        concurrentConnects = getMaxConcurrentResolvers(settings);
    }

    public static int getMaxConcurrentResolvers(Settings settings) {
        if (LEGACY_DISCOVERY_ZEN_PING_UNICAST_CONCURRENT_CONNECTS_SETTING.exists(settings)) {
            if (DISCOVERY_SEED_RESOLVER_MAX_CONCURRENT_RESOLVERS_SETTING.exists(settings)) {
                throw new IllegalArgumentException(
                    "it is forbidden to set both ["
                        + DISCOVERY_SEED_RESOLVER_MAX_CONCURRENT_RESOLVERS_SETTING.getKey()
                        + "] and ["
                        + LEGACY_DISCOVERY_ZEN_PING_UNICAST_CONCURRENT_CONNECTS_SETTING.getKey()
                        + "]"
                );
            }
            return LEGACY_DISCOVERY_ZEN_PING_UNICAST_CONCURRENT_CONNECTS_SETTING.get(settings);
        }
        return DISCOVERY_SEED_RESOLVER_MAX_CONCURRENT_RESOLVERS_SETTING.get(settings);
    }

    public static TimeValue getResolveTimeout(Settings settings) {
        if (LEGACY_DISCOVERY_ZEN_PING_UNICAST_HOSTS_RESOLVE_TIMEOUT.exists(settings)) {
            if (DISCOVERY_SEED_RESOLVER_TIMEOUT_SETTING.exists(settings)) {
                throw new IllegalArgumentException(
                    "it is forbidden to set both ["
                        + DISCOVERY_SEED_RESOLVER_TIMEOUT_SETTING.getKey()
                        + "] and ["
                        + LEGACY_DISCOVERY_ZEN_PING_UNICAST_HOSTS_RESOLVE_TIMEOUT.getKey()
                        + "]"
                );
            }
            return LEGACY_DISCOVERY_ZEN_PING_UNICAST_HOSTS_RESOLVE_TIMEOUT.get(settings);
        }
        return DISCOVERY_SEED_RESOLVER_TIMEOUT_SETTING.get(settings);
    }

    /**
     * Resolves a list of hosts to a list of transport addresses. Each host is resolved into a transport address (or a collection of
     * addresses if the number of ports is greater than one). Host lookups are done in parallel using specified executor service up
     * to the specified resolve timeout.
     *
     * @param executorService  the executor service used to parallelize hostname lookups
     * @param logger           logger used for logging messages regarding hostname lookups
     * @param hosts            the hosts to resolve
     * @param transportService the transport service
     * @param resolveTimeout   the timeout before returning from hostname lookups
     * @return a list of resolved transport addresses
     */
    public static List<TransportAddress> resolveHostsLists(
        final CancellableThreads cancellableThreads,
        final ExecutorService executorService,
        final Logger logger,
        final List<String> hosts,
        final TransportService transportService,
        final TimeValue resolveTimeout
    ) {
        Objects.requireNonNull(executorService);
        Objects.requireNonNull(logger);
        Objects.requireNonNull(hosts);
        Objects.requireNonNull(transportService);
        Objects.requireNonNull(resolveTimeout);
        if (resolveTimeout.nanos() < 0) {
            throw new IllegalArgumentException("resolve timeout must be non-negative but was [" + resolveTimeout + "]");
        }
        // create tasks to submit to the executor service; we will wait up to resolveTimeout for these tasks to complete
        final List<Callable<TransportAddress[]>> callables = hosts.stream()
            .map(hn -> (Callable<TransportAddress[]>) () -> transportService.addressesFromString(hn))
            .collect(Collectors.toList());
        final SetOnce<List<Future<TransportAddress[]>>> futures = new SetOnce<>();
        final long startTimeNanos = transportService.getThreadPool().relativeTimeInNanos();
        try {
            cancellableThreads.execute(
                () -> futures.set(executorService.invokeAll(callables, resolveTimeout.nanos(), TimeUnit.NANOSECONDS))
            );
        } catch (CancellableThreads.ExecutionCancelledException e) {
            return Collections.emptyList();
        }
        final TimeValue duration = TimeValue.timeValueNanos(transportService.getThreadPool().relativeTimeInNanos() - startTimeNanos);
        final List<TransportAddress> transportAddresses = new ArrayList<>();
        final Set<TransportAddress> localAddresses = new HashSet<>();
        localAddresses.add(transportService.boundAddress().publishAddress());
        localAddresses.addAll(Arrays.asList(transportService.boundAddress().boundAddresses()));
        // ExecutorService#invokeAll guarantees that the futures are returned in the iteration order of the tasks so we can associate the
        // hostname with the corresponding task by iterating together
        final Iterator<String> it = hosts.iterator();
        for (final Future<TransportAddress[]> future : futures.get()) {
            assert future.isDone();
            assert it.hasNext();
            final String hostname = it.next();
            if (future.isCancelled() == false) {
                try {
                    final TransportAddress[] addresses = future.get();
                    logger.trace("resolved host [{}] to {}", hostname, addresses);
                    for (final TransportAddress address : addresses) {
                        // no point in pinging ourselves
                        if (localAddresses.contains(address) == false) {
                            transportAddresses.add(address);
                        }
                    }
                } catch (final ExecutionException e) {
                    assert e.getCause() != null;
                    final String message = "failed to resolve host [" + hostname + "]";
                    logger.warn(message, e.getCause());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // ignore
                }
            } else {
                logger.warn(
                    "timed out after [{}/{}ms] ([{}]=[{}]) resolving host [{}]",
                    duration,
                    duration.getMillis(),
                    DISCOVERY_SEED_RESOLVER_TIMEOUT_SETTING.getKey(),
                    resolveTimeout,
                    hostname
                );
            }
        }
        return Collections.unmodifiableList(transportAddresses);
    }

    @Override
    protected void doStart() {
        logger.debug("using max_concurrent_resolvers [{}], resolver timeout [{}]", concurrentConnects, resolveTimeout);
        final ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(settings, "[unicast_configured_hosts_resolver]");
        executorService.set(
            EsExecutors.newScaling(
                nodeName + "/" + "unicast_configured_hosts_resolver",
                0,
                concurrentConnects,
                60,
                TimeUnit.SECONDS,
                false,
                threadFactory,
                transportService.getThreadPool().getThreadContext()
            )
        );
    }

    @Override
    protected void doStop() {
        cancellableThreads.cancel("stopping SeedHostsResolver");
        ThreadPool.terminate(executorService.get(), 10, TimeUnit.SECONDS);
    }

    @Override
    protected void doClose() {}

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

                    List<TransportAddress> providedAddresses = hostsProvider.getSeedAddresses(
                        hosts -> resolveHostsLists(
                            cancellableThreads,
                            executorService.get(),
                            logger,
                            hosts,
                            transportService,
                            resolveTimeout
                        )
                    );

                    consumer.accept(providedAddresses);
                }

                @Override
                public void onAfter() {
                    resolveInProgress.set(false);
                }

                @Override
                public String toString() {
                    return "SeedHostsResolver resolving unicast hosts list";
                }
            });
        }
    }

    List<TransportAddress> resolveHosts(List<String> hosts) {
        return resolveHostsLists(cancellableThreads, executorService.get(), logger, hosts, transportService, resolveTimeout);
    }
}
