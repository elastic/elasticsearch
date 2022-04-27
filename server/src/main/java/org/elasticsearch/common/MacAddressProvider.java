/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class MacAddressProvider {

    private static byte[] getMacAddress() throws SocketException {
        Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
        if (en != null) {
            while (en.hasMoreElements()) {
                NetworkInterface nint = en.nextElement();
                if (nint.isLoopback() == false) {
                    // Pick the first valid non loopback address we find
                    byte[] address = nint.getHardwareAddress();
                    if (isValidAddress(address)) {
                        return address;
                    }
                }
            }
        }
        // Could not find a mac address
        return null;
    }

    private static boolean isValidAddress(byte[] address) {
        if (address == null || address.length != 6) {
            return false;
        }
        for (byte b : address) {
            if (b != 0x00) {
                return true; // If any of the bytes are non zero assume a good address
            }
        }
        return false;
    }

    public static byte[] getSecureMungedAddress() {
        byte[] address = null;
        try {
            address = getMacAddress();
        } catch (SocketException e) {
            // address will be set below
        }

        if (isValidAddress(address) == false) {
            address = constructDummyMulticastAddress();
        }

        byte[] mungedBytes = new byte[6];
        SecureRandomHolder.INSTANCE.nextBytes(mungedBytes);
        for (int i = 0; i < 6; ++i) {
            mungedBytes[i] ^= address[i];
        }

        return mungedBytes;
    }

    private static byte[] constructDummyMulticastAddress() {
        byte[] dummy = new byte[6];
        SecureRandomHolder.INSTANCE.nextBytes(dummy);
        /*
         * Set the broadcast bit to indicate this is not a _real_ mac address
         */
        dummy[0] |= (byte) 0x01;
        return dummy;
    }

}
