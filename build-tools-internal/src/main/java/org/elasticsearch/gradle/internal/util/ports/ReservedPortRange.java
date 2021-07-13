/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.util.ports;

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
