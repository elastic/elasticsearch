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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AvailablePortAllocator implements PortAllocator {
    public static final int MIN_PRIVATE_PORT = 49152;
    public static final int MAX_PRIVATE_PORT = 65535;
    public static final int DEFAULT_RANGE_SIZE = 100;

    private final List<ReservedPortRange> reservations = new ArrayList<ReservedPortRange>();

    private final Lock lock = new ReentrantLock();

    private ReservedPortRangeFactory portRangeFactory = new DefaultReservedPortRangeFactory();

    @Override
    public int assignPort() {
        try {
            lock.lock();
            return reservePort();
        } finally {
            lock.unlock();
        }
    }

    protected Pair<Integer, Integer> getNextPortRange(int rangeNumber) {
        int startPort = MIN_PRIVATE_PORT + (rangeNumber * DEFAULT_RANGE_SIZE);
        int endPort = startPort + DEFAULT_RANGE_SIZE - 1;
        return Pair.of(startPort, endPort);
    }

    @Override
    public void releasePort(int port) {
        try {
            lock.lock();
            for (int i = 0; i < reservations.size(); i++) {
                ReservedPortRange range = reservations.get(i);
                if (range.getAllocated().contains(port)) {
                    range.deallocate(port);
                    if (reservations.size() > 1 && range.getAllocated().isEmpty()) {
                        releaseRange(range);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private int reservePort() {
        while (true) {
            for (int i = 0; i < reservations.size(); i++) {
                ReservedPortRange range = reservations.get(i);
                int port = range.allocate();
                if (port > 0) {
                    return port;
                }
            }

            // if we couldn't allocate a port from the existing reserved port ranges, get another range
            reservePortRange();
        }
    }

    public ReservedPortRange reservePortRange() {
        Pair<Integer, Integer> portRange = getNextPortRange(reservations.size());
        ReservedPortRange range = portRangeFactory.getReservedPortRange(portRange.getLeft(), portRange.getRight());
        reservations.add(range);
        return range;
    }

    private void releaseRange(ReservedPortRange range) {
        try {
            lock.lock();
            reservations.remove(range);
        } finally {
            lock.unlock();
        }
    }
}
