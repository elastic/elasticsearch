/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.util.ports;

import org.gradle.internal.Pair;

import java.util.ArrayList;
import java.util.List;

public class AvailablePortAllocator {
    public static final int MIN_PRIVATE_PORT = 10300;
    public static final int MAX_PRIVATE_PORT = 13300;
    public static final int DEFAULT_RANGE_SIZE = 10;

    private final List<ReservedPortRange> reservations = new ArrayList<ReservedPortRange>();

    private ReservedPortRangeFactory portRangeFactory = new DefaultReservedPortRangeFactory();

    protected Pair<Integer, Integer> getNextPortRange(int rangeNumber) {
        int startPort = MIN_PRIVATE_PORT + (rangeNumber * DEFAULT_RANGE_SIZE);
        int endPort = startPort + DEFAULT_RANGE_SIZE - 1;
        if (endPort > MAX_PRIVATE_PORT) {
            throw new IllegalStateException(
                "Available port ranges have exceeded Range from " + MIN_PRIVATE_PORT + " to " + MAX_PRIVATE_PORT
            );
        }
        return Pair.of(startPort, endPort);
    }

    public ReservedPortRange reservePortRange() {
        Pair<Integer, Integer> portRange = getNextPortRange(reservations.size());
        ReservedPortRange range = portRangeFactory.getReservedPortRange(portRange.getLeft(), portRange.getRight());
        reservations.add(range);
        return range;
    }
}
