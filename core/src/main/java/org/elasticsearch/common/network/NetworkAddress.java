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

package org.elasticsearch.common.network;

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
        return format(address, -1);
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

    // note, we don't validate port, because we only allow InetSocketAddress
    static String format(InetAddress address, int port) {
        Objects.requireNonNull(address);

        StringBuilder builder = new StringBuilder();

        if (port != -1 && address instanceof Inet6Address) {
            builder.append(InetAddresses.toUriString(address));
        } else {
            builder.append(InetAddresses.toAddrString(address));
        }

        if (port != -1) {
            builder.append(':');
            builder.append(port);
        }

        return builder.toString();
    }
}
