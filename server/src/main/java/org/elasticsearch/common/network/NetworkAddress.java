/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.network;

import org.elasticsearch.common.transport.PortsRange;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * Utility functions for presentation of network addresses.
 * <p>
 * Java's address formatting is particularly bad, every address
 * has an optional host if its resolved, so IPv4 addresses often
 * look like this (note the confusing leading slash):
 * <pre>
 *    {@code /127.0.0.1}
 * </pre>
 * IPv6 addresses are even worse, with no IPv6 address compression,
 * and often containing things like numeric scopeids, which are even
 * more confusing (e.g. not going to work in any user's browser, refer
 * to an interface on <b>another</b> machine, etc):
 * <pre>
 *    {@code /0:0:0:0:0:0:0:1%1}
 * </pre>
 * Note: the {@code %1} is the "scopeid".
 * <p>
 * This class provides sane address formatting instead, e.g.
 * {@code 127.0.0.1} and {@code ::1} respectively. No methods do reverse
 * lookups.
 */
public final class NetworkAddress {
    /** No instantiation */
    private NetworkAddress() {}

    /**
     * Formats a network address for display purposes.
     * <p>
     * This formats only the address, any hostname information,
     * if present, is ignored. IPv6 addresses are compressed
     * and without scope identifiers.
     * <p>
     * Example output with just an address:
     * <ul>
     *   <li>IPv4: {@code 127.0.0.1}</li>
     *   <li>IPv6: {@code ::1}</li>
     * </ul>
     * @param address IPv4 or IPv6 address
     * @return formatted string
     */
    public static String format(InetAddress address) {
        return format(address, new PortsRange(""));
    }

    /**
     * Formats a network address and port for display purposes.
     * <p>
     * This formats the address with {@link #format(InetAddress)}
     * and appends the port number. IPv6 addresses will be bracketed.
     * Any host information, if present is ignored.
     * <p>
     * Example output:
     * <ul>
     *   <li>IPv4: {@code 127.0.0.1:9300}</li>
     *   <li>IPv6: {@code [::1]:9300}</li>
     * </ul>
     * @param address IPv4 or IPv6 address with port
     * @return formatted string
     */
    public static String format(InetSocketAddress address) {
        return format(address.getAddress(), address.getPort());
    }

    /**
     * Formats a network address and port for display purposes.
     * <p>
     * This formats the address with {@link #format(InetAddress)}
     * and appends the port number. IPv6 addresses will be bracketed.
     * Any host information, if present is ignored.
     * <p>
     * Example output:
     * <ul>
     *   <li>IPv4: {@code 127.0.0.1:9300}</li>
     *   <li>IPv6: {@code [::1]:9300}</li>
     * </ul>
     * @param address IPv4 or IPv6 address
     * @param port port
     * @return formatted string
     */
    public static String format(InetAddress address, int port) {
        return format(address, new PortsRange(String.valueOf(port)));
    }

    /**
     * Formats a network address and port range for display purposes.
     * <p>
     * This formats the address with {@link #format(InetAddress)}
     * and appends the port range in brackets. In case there is only one
     * port, the result is the same with {@link #format(InetAddress, int)}.
     * <p>
     * Example output:
     * <ul>
     *   <li>IPv4 no port: {@code 127.0.0.1}</li>
     *   <li>IPv4 single port: {@code 127.0.0.1:9300}</li>
     *   <li>IPv4 multiple ports: {@code 127.0.0.1:[9300-9400]}</li>
     *   <li>IPv6 multiple ports: {@code [::1]:[9300-9400]}</li>
     * </ul>
     * @param address IPv4 or IPv6 address
     * @param portsRange range of ports
     * @return formatted string
     */
    public static String format(InetAddress address, PortsRange portsRange) {
        Objects.requireNonNull(address);

        StringBuilder builder = new StringBuilder();

        int numberOfPorts = portsRange.ports().length;

        if (numberOfPorts != 0 && address instanceof Inet6Address) {
            builder.append(InetAddresses.toUriString(address));
        } else {
            builder.append(InetAddresses.toAddrString(address));
        }

        if (numberOfPorts != 0) {
            builder.append(':');
            if (numberOfPorts == 1) {
                builder.append(portsRange.getPortRangeString());
            } else {
                builder.append("[").append(portsRange.getPortRangeString()).append("]");
            }
        }

        return builder.toString();
    }
}
