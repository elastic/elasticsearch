/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.network;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

public class NetworkDirectionUtils {

    static final byte[] UNDEFINED_IP4 = new byte[] { 0, 0, 0, 0 };
    static final byte[] UNDEFINED_IP6 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    static final byte[] BROADCAST_IP4 = new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff };

    private static final String LOOPBACK_NAMED_NETWORK = "loopback";
    private static final String GLOBAL_UNICAST_NAMED_NETWORK = "global_unicast";
    private static final String UNICAST_NAMED_NETWORK = "unicast";
    private static final String LINK_LOCAL_UNICAST_NAMED_NETWORK = "link_local_unicast";
    private static final String INTERFACE_LOCAL_NAMED_NETWORK = "interface_local_multicast";
    private static final String LINK_LOCAL_MULTICAST_NAMED_NETWORK = "link_local_multicast";
    private static final String MULTICAST_NAMED_NETWORK = "multicast";
    private static final String UNSPECIFIED_NAMED_NETWORK = "unspecified";
    private static final String PRIVATE_NAMED_NETWORK = "private";
    private static final String PUBLIC_NAMED_NETWORK = "public";

    public static final String DIRECTION_INTERNAL = "internal";
    public static final String DIRECTION_EXTERNAL = "external";
    public static final String DIRECTION_INBOUND = "inbound";
    public static final String DIRECTION_OUTBOUND = "outbound";

    public static boolean isInternal(List<String> networks, String ip) {
        for (String network : networks) {
            if (inNetwork(InetAddresses.forString(ip), network)) {
                return true;
            }
        }
        return false;
    }

    public static boolean inNetwork(InetAddress address, String network) {
        return switch (network) {
            case LOOPBACK_NAMED_NETWORK -> isLoopback(address);
            case GLOBAL_UNICAST_NAMED_NETWORK, UNICAST_NAMED_NETWORK -> isUnicast(address);
            case LINK_LOCAL_UNICAST_NAMED_NETWORK -> isLinkLocalUnicast(address);
            case INTERFACE_LOCAL_NAMED_NETWORK -> isInterfaceLocalMulticast(address);
            case LINK_LOCAL_MULTICAST_NAMED_NETWORK -> isLinkLocalMulticast(address);
            case MULTICAST_NAMED_NETWORK -> isMulticast(address);
            case UNSPECIFIED_NAMED_NETWORK -> isUnspecified(address);
            case PRIVATE_NAMED_NETWORK -> isPrivate(NetworkAddress.format(address));
            case PUBLIC_NAMED_NETWORK -> isPublic(NetworkAddress.format(address));
            default -> CIDRUtils.isInRange(NetworkAddress.format(address), network);
        };
    }

    public static String getDirection(boolean sourceInternal, boolean destinationInternal) {
        if (sourceInternal && destinationInternal) {
            return DIRECTION_INTERNAL;
        }
        if (sourceInternal) {
            return DIRECTION_OUTBOUND;
        }
        if (destinationInternal) {
            return DIRECTION_INBOUND;
        }
        return DIRECTION_EXTERNAL;
    }

    private static boolean isLoopback(InetAddress ip) {
        return ip.isLoopbackAddress();
    }

    private static boolean isUnicast(InetAddress ip) {
        return Arrays.equals(ip.getAddress(), BROADCAST_IP4) == false
            && isUnspecified(ip) == false
            && isLoopback(ip) == false
            && isMulticast(ip) == false
            && isLinkLocalUnicast(ip) == false;
    }

    private static boolean isLinkLocalUnicast(InetAddress ip) {
        return ip.isLinkLocalAddress();
    }

    private static boolean isInterfaceLocalMulticast(InetAddress ip) {
        return ip.isMCNodeLocal();
    }

    private static boolean isLinkLocalMulticast(InetAddress ip) {
        return ip.isMCLinkLocal();
    }

    private static boolean isMulticast(InetAddress ip) {
        return ip.isMulticastAddress();
    }

    private static boolean isUnspecified(InetAddress ip) {
        var address = ip.getAddress();
        return Arrays.equals(UNDEFINED_IP4, address) || Arrays.equals(UNDEFINED_IP6, address);
    }

    private static boolean isPrivate(String ip) {
        return CIDRUtils.isInRange(ip, "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "fd00::/8");
    }

    private static boolean isPublic(String ip) {
        return isLocalOrPrivate(ip) == false;
    }

    private static boolean isLocalOrPrivate(String ip) {
        var address = InetAddresses.forString(ip);
        return isPrivate(ip)
            || isLoopback(address)
            || isUnspecified(address)
            || isLinkLocalUnicast(address)
            || isLinkLocalMulticast(address)
            || isInterfaceLocalMulticast(address)
            || Arrays.equals(address.getAddress(), BROADCAST_IP4);
    }
}
