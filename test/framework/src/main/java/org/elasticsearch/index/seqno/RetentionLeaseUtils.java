/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
