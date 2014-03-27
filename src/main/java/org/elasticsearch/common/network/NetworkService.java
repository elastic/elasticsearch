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

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class NetworkService extends AbstractComponent {

    public static final String LOCAL = "#local#";

    private static final String GLOBAL_NETWORK_HOST_SETTING = "network.host";
    private static final String GLOBAL_NETWORK_BINDHOST_SETTING = "network.bind_host";
    private static final String GLOBAL_NETWORK_PUBLISHHOST_SETTING = "network.publish_host";

    public static final class TcpSettings {
        public static final String TCP_NO_DELAY = "network.tcp.no_delay";
        public static final String TCP_KEEP_ALIVE = "network.tcp.keep_alive";
        public static final String TCP_REUSE_ADDRESS = "network.tcp.reuse_address";
        public static final String TCP_SEND_BUFFER_SIZE = "network.tcp.send_buffer_size";
        public static final String TCP_RECEIVE_BUFFER_SIZE = "network.tcp.receive_buffer_size";
        public static final String TCP_BLOCKING = "network.tcp.blocking";
        public static final String TCP_BLOCKING_SERVER = "network.tcp.blocking_server";
        public static final String TCP_BLOCKING_CLIENT = "network.tcp.blocking_client";
        public static final String TCP_CONNECT_TIMEOUT = "network.tcp.connect_timeout";

        public static final ByteSizeValue TCP_DEFAULT_SEND_BUFFER_SIZE = null;
        public static final ByteSizeValue TCP_DEFAULT_RECEIVE_BUFFER_SIZE = null;
        public static final TimeValue TCP_DEFAULT_CONNECT_TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    }

    /**
     * A custom name resolver can support custom lookup keys (my_net_key:ipv4) and also change
     * the default inet address used in case no settings is provided.
     */
    public static interface CustomNameResolver {
        /**
         * Resolves the default value if possible. If not, return <tt>null</tt>.
         */
        InetAddress resolveDefault();

        /**
         * Resolves a custom value handling, return <tt>null</tt> if can't handle it.
         */
        InetAddress resolveIfPossible(String value);
    }

    private final List<CustomNameResolver> customNameResolvers = new CopyOnWriteArrayList<>();

    @Inject
    public NetworkService(Settings settings) {
        super(settings);
        InetSocketTransportAddress.setResolveAddress(settings.getAsBoolean("network.address.serialization.resolve", false));
    }

    /**
     * Add a custom name resolver.
     */
    public void addCustomNameResolver(CustomNameResolver customNameResolver) {
        customNameResolvers.add(customNameResolver);
    }


    public InetAddress resolveBindHostAddress(String bindHost) throws IOException {
        return resolveBindHostAddress(bindHost, null);
    }

    public InetAddress resolveBindHostAddress(String bindHost, String defaultValue2) throws IOException {
        return resolveInetAddress(bindHost, settings.get(GLOBAL_NETWORK_BINDHOST_SETTING, settings.get(GLOBAL_NETWORK_HOST_SETTING)), defaultValue2);
    }

    public InetAddress resolvePublishHostAddress(String publishHost) throws IOException {
        InetAddress address = resolvePublishHostAddress(publishHost, null);
        // verify that its not a local address
        if (address == null || address.isAnyLocalAddress()) {
            address = NetworkUtils.getFirstNonLoopbackAddress(NetworkUtils.StackType.IPv4);
            if (address == null) {
                address = NetworkUtils.getFirstNonLoopbackAddress(NetworkUtils.getIpStackType());
                if (address == null) {
                    address = NetworkUtils.getLocalAddress();
                    if (address == null) {
                        return NetworkUtils.getLocalhost(NetworkUtils.StackType.IPv4);
                    }
                }
            }
        }
        return address;
    }

    public InetAddress resolvePublishHostAddress(String publishHost, String defaultValue2) throws IOException {
        return resolveInetAddress(publishHost, settings.get(GLOBAL_NETWORK_PUBLISHHOST_SETTING, settings.get(GLOBAL_NETWORK_HOST_SETTING)), defaultValue2);
    }

    public InetAddress resolveInetAddress(String host, String defaultValue1, String defaultValue2) throws UnknownHostException, IOException {
        if (host == null) {
            host = defaultValue1;
        }
        if (host == null) {
            host = defaultValue2;
        }
        if (host == null) {
            for (CustomNameResolver customNameResolver : customNameResolvers) {
                InetAddress inetAddress = customNameResolver.resolveDefault();
                if (inetAddress != null) {
                    return inetAddress;
                }
            }
            return null;
        }
        String origHost = host;
        if ((host.startsWith("#") && host.endsWith("#")) || (host.startsWith("_") && host.endsWith("_"))) {
            host = host.substring(1, host.length() - 1);

            for (CustomNameResolver customNameResolver : customNameResolvers) {
                InetAddress inetAddress = customNameResolver.resolveIfPossible(host);
                if (inetAddress != null) {
                    return inetAddress;
                }
            }

            if (host.equals("local")) {
                return NetworkUtils.getLocalAddress();
            } else if (host.startsWith("non_loopback")) {
                if (host.toLowerCase(Locale.ROOT).endsWith(":ipv4")) {
                    return NetworkUtils.getFirstNonLoopbackAddress(NetworkUtils.StackType.IPv4);
                } else if (host.toLowerCase(Locale.ROOT).endsWith(":ipv6")) {
                    return NetworkUtils.getFirstNonLoopbackAddress(NetworkUtils.StackType.IPv6);
                } else {
                    return NetworkUtils.getFirstNonLoopbackAddress(NetworkUtils.getIpStackType());
                }
            } else {
                NetworkUtils.StackType stackType = NetworkUtils.getIpStackType();
                if (host.toLowerCase(Locale.ROOT).endsWith(":ipv4")) {
                    stackType = NetworkUtils.StackType.IPv4;
                    host = host.substring(0, host.length() - 5);
                } else if (host.toLowerCase(Locale.ROOT).endsWith(":ipv6")) {
                    stackType = NetworkUtils.StackType.IPv6;
                    host = host.substring(0, host.length() - 5);
                }
                Collection<NetworkInterface> allInterfs = NetworkUtils.getAllAvailableInterfaces();
                for (NetworkInterface ni : allInterfs) {
                    if (!ni.isUp()) {
                        continue;
                    }
                    if (host.equals(ni.getName()) || host.equals(ni.getDisplayName())) {
                        if (ni.isLoopback()) {
                            return NetworkUtils.getFirstAddress(ni, stackType);
                        } else {
                            return NetworkUtils.getFirstNonLoopbackAddress(ni, stackType);
                        }
                    }
                }
            }
            throw new IOException("Failed to find network interface for [" + origHost + "]");
        }
        return InetAddress.getByName(host);
    }
}
