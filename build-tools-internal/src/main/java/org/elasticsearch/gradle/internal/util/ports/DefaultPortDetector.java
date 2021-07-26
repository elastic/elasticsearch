/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.util.ports;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;

public class DefaultPortDetector implements PortDetector {
    /**
     * Checks to see if a specific port is available.
     *
     * @param port the port to check for availability
     * @return true if the port is available, false otherwise
     */
    public boolean isAvailable(int port) {
        try {
            ServerSocket ss = new ServerSocket(port);
            try {
                ss.setReuseAddress(true);
            } finally {
                ss.close();
            }

            DatagramSocket ds = new DatagramSocket(port);
            try {
                ds.setReuseAddress(true);
            } finally {
                ds.close();
            }

            return true;
        } catch (IOException e) {
            return false;
        }

    }

}
