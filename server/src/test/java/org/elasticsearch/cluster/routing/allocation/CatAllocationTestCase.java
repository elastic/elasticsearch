/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;

/**
 * A base testcase that allows to run tests based on the output of the CAT API
 * The input is a line based cat/shards output like:
 *   kibana-int           0 p STARTED       2  24.8kb 10.202.245.2 r5-9-35
 *
 * the test builds up a clusterstate from the cat input and optionally runs a full balance on it.
 * This can be used to debug cluster allocation decisions.
 */
public abstract class CatAllocationTestCase extends ESAllocationTestCase {
    protected abstract Path getCatPath() throws IOException;

    public void testRun() throws IOException {
        Set<String> nodes = new HashSet<>();
        Map<String, Idx> indices = new HashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(getCatPath(), StandardCharsets.UTF_8)) {
            String line = null;
            // regexp FTW
            Pattern pattern = Pattern.compile("^(.+)\\s+(\\d)\\s+([rp])\\s+(STARTED|RELOCATING|INITIALIZING|UNASSIGNED)" +
                "\\s+\\d+\\s+[0-9.a-z]+\\s+(\\d+\\.\\d+\\.\\d+\\.\\d+).*$");
            while((line = reader.readLine()) != null) {
                final Matcher matcher;
                if ((matcher = pattern.matcher(line)).matches()) {
                    final String index = matcher.group(1);
                    Idx idx = indices.get(index);
                    if (idx == null) {
                        idx = new Idx(index);
                        indices.put(index, idx);
                    }
                    final int shard = Integer.parseInt(matcher.group(2));
                    final boolean primary = matcher.group(3).equals("p");
                    ShardRoutingState state = ShardRoutingState.valueOf(matcher.group(4));
                    String ip = matcher.group(5);
                    nodes.add(ip);
                    ShardRouting routing = TestShardRouting.newShardRouting(index, shard, ip, null, primary, state);
                    idx.add(routing);
                    logger.debug("Add routing {}", routing);
                } else {
                    fail("can't read line: " + line);
                }
            }

        }

        logger.info("Building initial routing table");
        Metadata.Builder builder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        for(Idx idx : indices.values()) {
            IndexMetadata.Builder idxMetaBuilder = IndexMetadata.builder(idx.name).settings(settings(Version.CURRENT))
                .numberOfShards(idx.numShards()).numberOfReplicas(idx.numReplicas());
            for (ShardRouting shardRouting : idx.routing) {
                if (shardRouting.active()) {
                    Set<String> allocationIds = idxMetaBuilder.getInSyncAllocationIds(shardRouting.id());
                    if (allocationIds == null) {
                        allocationIds = new HashSet<>();
                    } else {
                        allocationIds = new HashSet<>(allocationIds);
                    }
                    allocationIds.add(shardRouting.allocationId().getId());
                    idxMetaBuilder.putInSyncAllocationIds(shardRouting.id(), allocationIds);
                }
            }
            IndexMetadata idxMeta = idxMetaBuilder.build();
            builder.put(idxMeta, false);
            IndexRoutingTable.Builder tableBuilder = new IndexRoutingTable.Builder(idxMeta.getIndex()).initializeAsRecovery(idxMeta);
            Map<Integer, IndexShardRoutingTable> shardIdToRouting = new HashMap<>();
            for (ShardRouting r : idx.routing) {
                IndexShardRoutingTable refData = new IndexShardRoutingTable.Builder(r.shardId()).addShard(r).build();
                if (shardIdToRouting.containsKey(r.getId())) {
                    refData = new IndexShardRoutingTable.Builder(shardIdToRouting.get(r.getId())).addShard(r).build();
                }
                shardIdToRouting.put(r.getId(), refData);
            }
            for (IndexShardRoutingTable t: shardIdToRouting.values()) {
                tableBuilder.addIndexShard(t);
            }
            IndexRoutingTable table = tableBuilder.build();
            routingTableBuilder.add(table);
        }
        Metadata metadata = builder.build();

        RoutingTable routingTable = routingTableBuilder.build();
        DiscoveryNodes.Builder builderDiscoNodes = DiscoveryNodes.builder();
        for (String node : nodes) {
            builderDiscoNodes.add(newNode(node));
        }
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(routingTable).nodes(builderDiscoNodes.build()).build();
        if (balanceFirst()) {
            clusterState = rebalance(clusterState);
        }
        clusterState = allocateNew(clusterState);
    }

    protected abstract ClusterState allocateNew(ClusterState clusterState);

    protected boolean balanceFirst() {
        return true;
    }

    private ClusterState rebalance(ClusterState clusterState) {
        AllocationService strategy = createAllocationService(Settings.builder()
                .build());
        clusterState = strategy.reroute(clusterState, "reroute");
        int numRelocations = 0;
        while (true) {
            List<ShardRouting> initializing = clusterState.routingTable().shardsWithState(INITIALIZING);
            if (initializing.isEmpty()) {
                break;
            }
            logger.debug("Initializing shards: {}", initializing);
            numRelocations += initializing.size();
            clusterState = ESAllocationTestCase.startShardsAndReroute(strategy, clusterState, initializing);
        }
        logger.debug("--> num relocations to get balance: {}", numRelocations);
        return clusterState;
    }



    public class Idx {
        final String name;
        final List<ShardRouting> routing = new ArrayList<>();

        public Idx(String name) {
            this.name = name;
        }


        public void add(ShardRouting r) {
            routing.add(r);
        }

        public int numReplicas() {
            int count = 0;
            for (ShardRouting msr : routing) {
                if (msr.primary() == false && msr.id()==0) {
                    count++;
                }
            }
            return count;
        }

        public int numShards() {
            int max = 0;
            for (ShardRouting msr : routing) {
                if (msr.primary()) {
                    max = Math.max(msr.getId()+1, max);
                }
            }
            return max;
        }
    }
}
