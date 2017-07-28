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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

/**
 * A pluggable provider of the list of unicast hosts to use for unicast discovery.
 */
public interface UnicastHostsProvider {

    Setting<TimeValue> DISCOVERY_ZEN_PING_UNICAST_HOSTS_RESOLVE_TIMEOUT = Setting.positiveTimeSetting(
        "discovery.zen.ping.unicast.hosts.resolve_timeout", TimeValue.timeValueSeconds(5), Setting.Property.NodeScope);

    Setting<Integer> DISCOVERY_ZEN_PING_UNICAST_CONCURRENT_CONNECTS_SETTING =
            Setting.intSetting("discovery.zen.ping.unicast.concurrent_connects", 10, 0, Setting.Property.NodeScope);

    /**
     * Resolves a list of hosts to a list of discovery nodes. Each host is resolved into a transport address (or a collection of addresses
     * if the number of ports is greater than one) and the transport addresses are used to created discovery nodes. Host lookups are done
     * in parallel using specified executor service up to the specified resolve timeout.
     *
     * @param executorService  the executor service used to parallelize hostname lookups
     * @param logger           logger used for logging messages regarding hostname lookups
     * @param hosts            the hosts to resolve
     * @param limitPortCounts  the number of ports to resolve (should be 1 for non-local transport)
     * @param transportService the transport service
     * @param nodeId_prefix    a prefix to use for node ids
     * @param resolveTimeout   the timeout before returning from hostname lookups
     * @return a list of discovery nodes with resolved transport addresses
     */
    static List<DiscoveryNode> resolveHostsLists(
        final ExecutorService executorService,
        final Logger logger,
        final List<String> hosts,
        final int limitPortCounts,
        final TransportService transportService,
        final String nodeId_prefix,
        final TimeValue resolveTimeout) throws InterruptedException {
        Objects.requireNonNull(executorService);
        Objects.requireNonNull(logger);
        Objects.requireNonNull(hosts);
        Objects.requireNonNull(transportService);
        Objects.requireNonNull(nodeId_prefix);
        Objects.requireNonNull(resolveTimeout);
        if (resolveTimeout.nanos() < 0) {
            throw new IllegalArgumentException("resolve timeout must be non-negative but was [" + resolveTimeout + "]");
        }
        // create tasks to submit to the executor service; we will wait up to resolveTimeout for these tasks to complete
        final List<Callable<TransportAddress[]>> callables =
            hosts
                .stream()
                .map(hn -> (Callable<TransportAddress[]>) () -> transportService.addressesFromString(hn, limitPortCounts))
                .collect(Collectors.toList());
        final List<Future<TransportAddress[]>> futures =
            executorService.invokeAll(callables, resolveTimeout.nanos(), TimeUnit.NANOSECONDS);
        final List<DiscoveryNode> discoveryNodes = new ArrayList<>();
        final Set<TransportAddress> localAddresses = new HashSet<>();
        localAddresses.add(transportService.boundAddress().publishAddress());
        localAddresses.addAll(Arrays.asList(transportService.boundAddress().boundAddresses()));
        // ExecutorService#invokeAll guarantees that the futures are returned in the iteration order of the tasks so we can associate the
        // hostname with the corresponding task by iterating together
        final Iterator<String> it = hosts.iterator();
        for (final Future<TransportAddress[]> future : futures) {
            final String hostname = it.next();
            if (!future.isCancelled()) {
                assert future.isDone();
                try {
                    final TransportAddress[] addresses = future.get();
                    logger.trace("resolved host [{}] to {}", hostname, addresses);
                    for (int addressId = 0; addressId < addresses.length; addressId++) {
                        final TransportAddress address = addresses[addressId];
                        // no point in pinging ourselves
                        if (localAddresses.contains(address) == false) {
                            discoveryNodes.add(
                                new DiscoveryNode(
                                    nodeId_prefix + hostname + "_" + addressId + "#",
                                    address,
                                    emptyMap(),
                                    emptySet(),
                                    Version.CURRENT.minimumCompatibilityVersion()));
                        }
                    }
                } catch (final ExecutionException e) {
                    assert e.getCause() != null;
                    final String message = "failed to resolve host [" + hostname + "]";
                    logger.warn(message, e.getCause());
                }
            } else {
                logger.warn("timed out after [{}] resolving host [{}]", resolveTimeout, hostname);
            }
        }
        return discoveryNodes;
    }

    /**
     * Builds the dynamic list of unicast hosts to be used for unicast discovery.
     */
    List<DiscoveryNode> buildDynamicNodes();
}
