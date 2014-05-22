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

package org.elasticsearch.action.benchmark;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.BenchmarkMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.inject.Inject;

import java.util.ArrayList;
import java.util.List;

/**
 * Various benchmark utility methods
 */
public class BenchmarkUtility {

    private final ClusterService clusterService;

    @Inject
    public BenchmarkUtility(final ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public static boolean exists(String benchmarkId, BenchmarkMetaData meta) {
        if (meta != null) {
            for (BenchmarkMetaData.Entry entry : meta.entries()) {
                if (entry.benchmarkId().equals(benchmarkId)) {
                    return true;
                }
            }
        }
        return false;
    }

    public List<DiscoveryNode> executors(int num) {
        assert num >= 1;
        final List<DiscoveryNode> executors = new ArrayList<>();
        final DiscoveryNodes nodes = clusterService.state().nodes();

        for (DiscoveryNode node : nodes) {
            if (executors.size() == num) {
                break;
            }
            if (isBenchmarkNode(node)) {
                executors.add(node);
            }
        }
        return executors;
    }

    public static boolean isBenchmarkNode(DiscoveryNode node) {
        ImmutableMap<String, String> attributes = node.getAttributes();
        if (attributes.containsKey("bench")) {
            String bench = attributes.get("bench");
            return Boolean.parseBoolean(bench);
        }
        return false;
    }
}
