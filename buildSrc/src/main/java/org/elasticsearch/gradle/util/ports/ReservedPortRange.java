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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ReservedPortRange {
    private final int startPort;
    private final int endPort;
    private final Lock lock = new ReentrantLock();
    private PortDetector portDetector = new DefaultPortDetector();
    private final List<Integer> allocated = new ArrayList<Integer>();
    private Map<String, Integer> allocatedPortsId = new HashMap<>();
    private int current;

    public ReservedPortRange(int startPort, int endPort) {
        this.startPort = startPort;
        this.endPort = endPort;
        current = startPort + new Random().nextInt(endPort - startPort);
    }

    public List<Integer> getAllocated() {
        return allocated;
    }

    public final Integer getOrAllocate(String id) {
        return allocatedPortsId.computeIfAbsent(id, (key) -> allocate());
    }

    public final Integer getAllocated(String id) {
        return allocatedPortsId.get(id);
    }

    public int getCurrent() {
        return current;
    }

    public void setCurrent(int current) {
        this.current = current;
    }

    /**
     * Allocate an available port
     *
     * @return the port that was allocated
     */
    int allocate() {
        try {
            lock.lock();
            return getAvailablePort();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Deallocate the given port
     *
     * @param port The port to deallocate
     */
    public void deallocate(int port) {
        try {
            lock.lock();
            allocated.remove(Integer.valueOf(port));
        } finally {
            lock.unlock();
        }
    }

    private int getAvailablePort() {
        int first = current;
        while (true) {
            current++;
            if (current > endPort) {
                current = startPort;
            }
            int candidate = current;
            if (allocated.contains(candidate) == false && portDetector.isAvailable(candidate)) {
                allocated.add(candidate);
                return candidate;
            } else {
                if (current == first) {
                    return -1;
                }
            }
        }
    }

    @Override
    public String toString() {
        return "ReservedPortRange[" + startPort + ":" + endPort + "]";
    }

}
