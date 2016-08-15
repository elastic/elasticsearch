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

package org.elasticsearch.usage;

import org.elasticsearch.action.admin.cluster.node.usage.NodeUsage;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * A service to monitor usage of Elasticsearch features.
 */
public class UsageService extends AbstractComponent {

    private final Supplier<DiscoveryNode> localNodeSupplier;
    private final Map<String, AtomicLong> restUsage;
    private long sinceTime;

    @Inject
    public UsageService(Supplier<DiscoveryNode> localNodeSupplier, Settings settings) {
        super(settings);
        this.localNodeSupplier = localNodeSupplier;
        this.restUsage = new ConcurrentHashMap<>();
        this.sinceTime = System.currentTimeMillis();
    }

    /**
     * record a call to a REST endpoint.
     *
     * @param actionName
     *            the class name of the {@link RestHandler} called for this
     *            endpoint.
     */
    public void addRestCall(String actionName) {
        AtomicLong counter = restUsage.computeIfAbsent(actionName, key -> new AtomicLong());
        counter.getAndIncrement();
    }

    public void clear() {
        this.sinceTime = System.currentTimeMillis();
        this.restUsage.clear();
    }

    /**
     * Get the current usage statistics for this node.
     *
     * @param restActions
     *            whether to include rest action usage in the returned
     *            statistics
     * @return the {@link NodeUsage} representing the usage statistics for this
     *         node
     */
    public NodeUsage getUsageStats(boolean restActions) {
        Map<String, Long> restUsageMap;
        if (restActions) {
            restUsageMap = new HashMap<>();
            restUsage.forEach((key, value) -> {
                restUsageMap.put(key, value.get());
            });
        } else {
            restUsageMap = null;
        }
        return new NodeUsage(localNodeSupplier.get(), System.currentTimeMillis(), sinceTime, restUsageMap);
    }

}
