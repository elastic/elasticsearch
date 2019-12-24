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
package org.elasticsearch.index.seqno;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.junit.Assert;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.hasItems;

public class RetentionLeaseUtils {

    private RetentionLeaseUtils() {
        // only static methods
    }

    /**
     * A utility method to convert a retention lease collection to a map from retention lease ID to retention lease and exclude
     * the automatically-added peer-recovery retention leases
     *
     * @param retentionLeases the retention lease collection
     * @return the map from retention lease ID to retention lease
     */
    public static Map<String, RetentionLease> toMapExcludingPeerRecoveryRetentionLeases(final RetentionLeases retentionLeases) {
        return retentionLeases.leases().stream()
            .filter(l -> ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE.equals(l.source()) == false)
            .collect(Collectors.toMap(RetentionLease::id, Function.identity(),
                (o1, o2) -> {
                    throw new AssertionError("unexpectedly merging " + o1 + " and " + o2);
                },
                LinkedHashMap::new));
    }

    /**
     * Asserts that every copy of every shard of the given index has a peer recovery retention lease according to the stats exposed by the
     * REST API
     */
    public static void assertAllCopiesHavePeerRecoveryRetentionLeases(RestClient restClient, String index) throws IOException {
        final Request statsRequest = new Request("GET", "/" + index + "/_stats");
        statsRequest.addParameter("level", "shards");
        final Map<?, ?> shardsStats = ObjectPath.createFromResponse(restClient.performRequest(statsRequest))
            .evaluate("indices." + index + ".shards");
        for (Map.Entry<?, ?> shardCopiesEntry : shardsStats.entrySet()) {
            final List<?> shardCopiesList = (List<?>) shardCopiesEntry.getValue();

            final Set<String> expectedLeaseIds = new HashSet<>();
            for (Object shardCopyStats : shardCopiesList) {
                final String nodeId
                    = Objects.requireNonNull((String) ((Map<?, ?>) (((Map<?, ?>) shardCopyStats).get("routing"))).get("node"));
                expectedLeaseIds.add(ReplicationTracker.getPeerRecoveryRetentionLeaseId(
                    ShardRouting.newUnassigned(new ShardId("_na_", "test", 0), false, RecoverySource.PeerRecoverySource.INSTANCE,
                        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test")).initialize(nodeId, null, 0L)));
            }

            final Set<String> actualLeaseIds = new HashSet<>();
            for (Object shardCopyStats : shardCopiesList) {
                final List<?> leases
                    = (List<?>) ((Map<?, ?>) (((Map<?, ?>) shardCopyStats).get("retention_leases"))).get("leases");
                for (Object lease : leases) {
                    actualLeaseIds.add(Objects.requireNonNull((String) (((Map<?, ?>) lease).get("id"))));
                }
            }
            Assert.assertThat("[" + index + "][" + shardCopiesEntry.getKey() + "] has leases " + actualLeaseIds
                    + " but expected " + expectedLeaseIds,
                actualLeaseIds, hasItems(expectedLeaseIds.toArray(new String[0])));
        }
    }
}
