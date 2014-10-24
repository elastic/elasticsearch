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
package org.elasticsearch.test.transport;

import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.network.NetworkUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.Locale;

import static org.elasticsearch.common.Preconditions.checkArgument;

/**
 * helper class to find a free port
 */
public class SocketUtil {

    /**
     * Find a number of free ports in the IANA ephemeral port range [49152-65535]
     *
     * @param numberOfPorts Number of ports to find
     * @return an array of integer which are considered to be free when tested
     */
    public static int[] findFreeLocalPorts(int numberOfPorts) {
        // IANA suggests to use these as "ephemeral ports" (short lived)
        return findFreeLocalPorts(49152, 65535, numberOfPorts);
    }

    /**
     * Find a single free port in the IANA ephemeral port range [49152-65535]
     *
     * @return An int representing a port which was considered to be free when tested
     */
    public static int findFreeLocalPort() {
        // IANA suggests to use these as "ephemeral ports" (short lived)
        return findFreeLocalPorts(49152, 65535, 1)[0];
    }

    /**
     * Find a number of free ports in the configured port range
     *
     * @param low Lowest port in allowed range
     * @param high Max port in allowed range
     * @param numberOfPorts Number of ports to find
     * @return an array of integer which are considered to be free when tested
     */
    public static int[] findFreeLocalPorts(int low, int high, int numberOfPorts) {
        checkArgument(low < high, String.format(Locale.ROOT, "Expected %s < %s", low, high));
        IntOpenHashSet ports = new IntOpenHashSet();

        // create some exit condition vars
        int runs = 0;
        int maxRuns = numberOfPorts * 10;

        while (ports.size() < numberOfPorts) {
            int randomPort = RandomizedTest.randomIntBetween(low, high);
            if (isFreeSocket(randomPort)) {
                ports.add(randomPort);
            }

            if (++runs > maxRuns) {
                throw new ElasticsearchException("Could not find free port in range[" + low + "-" + high + "] after " + runs + " runs");
            }
        }

        return ports.toArray();
    }

    /**
     * Try a socket bind and close to see if the port is free on the local interface
     * @param port A an arbitrary port number
     *
     * @return true if the socket could be bound, false otherwise
     */
    private static boolean isFreeSocket(int port) {
        InetAddress localAddress = NetworkUtils.getLocalAddress();

        try {
            try(ServerSocket serverSocket = new ServerSocket()) {
                SocketAddress sa = new InetSocketAddress(localAddress, port);
                serverSocket.setReuseAddress(true);
                serverSocket.bind(sa);
                return true;
            }
        } catch (IOException e) {
            // ignore and try to use the next port
        }

        return false;
    }
}
