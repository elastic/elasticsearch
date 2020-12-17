/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.util.ports;

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
