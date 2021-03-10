/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.transport;


import com.carrotsearch.hppc.IntArrayList;

import java.util.StringTokenizer;

public class PortsRange {

    private final String portRange;

    public PortsRange(String portRange) {
        this.portRange = portRange;
    }

    public String getPortRangeString() {
        return portRange;
    }

    public int[] ports() throws NumberFormatException {
        final IntArrayList ports = new IntArrayList();
        iterate(new PortCallback() {
            @Override
            public boolean onPortNumber(int portNumber) {
                ports.add(portNumber);
                return false;
            }
        });
        return ports.toArray();
    }

    public boolean iterate(PortCallback callback) throws NumberFormatException {
        StringTokenizer st = new StringTokenizer(portRange, ",");
        boolean success = false;
        while (st.hasMoreTokens() && success == false) {
            String portToken = st.nextToken().trim();
            int index = portToken.indexOf('-');
            if (index == -1) {
                int portNumber = Integer.parseInt(portToken.trim());
                success = callback.onPortNumber(portNumber);
                if (success) {
                    break;
                }
            } else {
                int startPort = Integer.parseInt(portToken.substring(0, index).trim());
                int endPort = Integer.parseInt(portToken.substring(index + 1).trim());
                if (endPort < startPort) {
                    throw new IllegalArgumentException("Start port [" + startPort + "] must be greater than end port [" + endPort + "]");
                }
                for (int i = startPort; i <= endPort; i++) {
                    success = callback.onPortNumber(i);
                    if (success) {
                        break;
                    }
                }
            }
        }
        return success;
    }

    public interface PortCallback {
        boolean onPortNumber(int portNumber);
    }

    @Override
    public String toString() {
        return "PortsRange{" +
            "portRange='" + portRange + '\'' +
            '}';
    }
}
