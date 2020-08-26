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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

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
}
