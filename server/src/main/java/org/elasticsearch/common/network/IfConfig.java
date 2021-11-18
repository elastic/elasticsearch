/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.network;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.List;
import java.util.Locale;

/**
 * Simple class to log {@code ifconfig}-style output at DEBUG logging.
 */
public final class IfConfig {

    private static final Logger logger = LogManager.getLogger(IfConfig.class);
    private static final String INDENT = "        ";

    /** log interface configuration at debug level, if its enabled */
    public static void logIfNecessary() {
        if (logger.isDebugEnabled()) {
            try {
                doLogging();
            } catch (IOException e) {
                logger.warn("unable to gather network information", e);
            }
        }
    }

    /** perform actual logging: might throw exception if things go wrong */
    private static void doLogging() throws IOException {
        StringBuilder msg = new StringBuilder();
        for (NetworkInterface nic : NetworkUtils.getInterfaces()) {
            msg.append(System.lineSeparator());

            // ordinary name
            msg.append(nic.getName());
            msg.append(System.lineSeparator());

            // display name (e.g. on windows)
            if (nic.getName().equals(nic.getDisplayName()) == false) {
                msg.append(INDENT);
                msg.append(nic.getDisplayName());
                msg.append(System.lineSeparator());
            }

            // addresses: v4 first, then v6
            List<InterfaceAddress> addresses = nic.getInterfaceAddresses();
            for (InterfaceAddress address : addresses) {
                if (address.getAddress() instanceof Inet6Address == false) {
                    msg.append(INDENT);
                    msg.append(formatAddress(address));
                    msg.append(System.lineSeparator());
                }
            }

            for (InterfaceAddress address : addresses) {
                if (address.getAddress() instanceof Inet6Address) {
                    msg.append(INDENT);
                    msg.append(formatAddress(address));
                    msg.append(System.lineSeparator());
                }
            }

            // hardware address
            byte hardware[] = nic.getHardwareAddress();
            if (hardware != null) {
                msg.append(INDENT);
                msg.append("hardware ");
                for (int i = 0; i < hardware.length; i++) {
                    if (i > 0) {
                        msg.append(":");
                    }
                    msg.append(String.format(Locale.ROOT, "%02X", hardware[i]));
                }
                msg.append(System.lineSeparator());
            }

            // attributes
            msg.append(INDENT);
            msg.append(formatFlags(nic));
            msg.append(System.lineSeparator());
        }
        logger.debug("configuration:{}{}", System.lineSeparator(), msg);
    }

    /** format internet address: java's default doesn't include everything useful */
    private static String formatAddress(InterfaceAddress interfaceAddress) throws IOException {
        StringBuilder sb = new StringBuilder();

        InetAddress address = interfaceAddress.getAddress();
        if (address instanceof Inet6Address) {
            sb.append("inet6 ");
            sb.append(NetworkAddress.format(address));
            sb.append(" prefixlen:");
            sb.append(interfaceAddress.getNetworkPrefixLength());
        } else {
            sb.append("inet ");
            sb.append(NetworkAddress.format(address));
            int netmask = 0xFFFFFFFF << (32 - interfaceAddress.getNetworkPrefixLength());
            sb.append(" netmask:")
                .append(
                    NetworkAddress.format(
                        InetAddress.getByAddress(
                            new byte[] {
                                (byte) (netmask >>> 24),
                                (byte) (netmask >>> 16 & 0xFF),
                                (byte) (netmask >>> 8 & 0xFF),
                                (byte) (netmask & 0xFF) }
                        )
                    )
                );
            InetAddress broadcast = interfaceAddress.getBroadcast();
            if (broadcast != null) {
                sb.append(" broadcast:").append(NetworkAddress.format(broadcast));
            }
        }
        if (address.isLoopbackAddress()) {
            sb.append(" scope:host");
        } else if (address.isLinkLocalAddress()) {
            sb.append(" scope:link");
        } else if (address.isSiteLocalAddress()) {
            sb.append(" scope:site");
        }
        return sb.toString();
    }

    /** format network interface flags */
    private static String formatFlags(NetworkInterface nic) throws SocketException {
        StringBuilder flags = new StringBuilder();
        if (nic.isUp()) {
            flags.append("UP ");
        }
        if (nic.supportsMulticast()) {
            flags.append("MULTICAST ");
        }
        if (nic.isLoopback()) {
            flags.append("LOOPBACK ");
        }
        if (nic.isPointToPoint()) {
            flags.append("POINTOPOINT ");
        }
        if (nic.isVirtual()) {
            flags.append("VIRTUAL ");
        }
        flags.append("mtu:").append(nic.getMTU());
        flags.append(" index:").append(nic.getIndex());
        return flags.toString();
    }
}
