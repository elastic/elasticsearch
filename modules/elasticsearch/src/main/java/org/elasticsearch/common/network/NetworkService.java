/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * @author kimchy (shay.banon)
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

        public static final ByteSizeValue TCP_DEFAULT_SEND_BUFFER_SIZE = new ByteSizeValue(32, ByteSizeUnit.KB);
        public static final ByteSizeValue TCP_DEFAULT_RECEIVE_BUFFER_SIZE = new ByteSizeValue(32, ByteSizeUnit.KB);
        public static final TimeValue TCP_DEFAULT_CONNECT_TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    }

    public static interface CustomNameResolver {
        InetAddress resolve();
    }

    private volatile ImmutableMap<String, CustomNameResolver> customNameResolvers = ImmutableMap.of();

    @Inject public NetworkService(Settings settings) {
        super(settings);
    }

    public void addCustomNameResolver(String name, CustomNameResolver customNameResolver) {
        customNameResolvers = MapBuilder.<String, CustomNameResolver>newMapBuilder().putAll(customNameResolvers).put(name, customNameResolver).immutableMap();
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
                    return NetworkUtils.getLocalAddress();
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
            return null;
        }
        String origHost = host;
        if ((host.startsWith("#") && host.endsWith("#")) || (host.startsWith("_") && host.endsWith("_"))) {
            host = host.substring(1, host.length() - 1);

            CustomNameResolver customNameResolver = customNameResolvers.get(host);
            if (customNameResolver != null) {
                return customNameResolver.resolve();
            }

            if (host.equals("local")) {
                return NetworkUtils.getLocalAddress();
            } else if (host.startsWith("non_loopback")) {
                if (host.toLowerCase().endsWith(":ipv4")) {
                    return NetworkUtils.getFirstNonLoopbackAddress(NetworkUtils.StackType.IPv4);
                } else if (host.toLowerCase().endsWith(":ipv6")) {
                    return NetworkUtils.getFirstNonLoopbackAddress(NetworkUtils.StackType.IPv6);
                } else {
                    return NetworkUtils.getFirstNonLoopbackAddress(NetworkUtils.getIpStackType());
                }
            } else {
                NetworkUtils.StackType stackType = NetworkUtils.getIpStackType();
                if (host.toLowerCase().endsWith(":ipv4")) {
                    stackType = NetworkUtils.StackType.IPv4;
                    host = host.substring(0, host.length() - 5);
                } else if (host.toLowerCase().endsWith(":ipv6")) {
                    stackType = NetworkUtils.StackType.IPv6;
                    host = host.substring(0, host.length() - 5);
                }
                Collection<NetworkInterface> allInterfs = NetworkUtils.getAllAvailableInterfaces();
                for (NetworkInterface ni : allInterfs) {
                    if (!ni.isUp() || ni.isLoopback()) {
                        continue;
                    }
                    if (host.equals(ni.getName()) || host.equals(ni.getDisplayName())) {
                        return NetworkUtils.getFirstNonLoopbackAddress(ni, stackType);
                    }
                }
            }
            throw new IOException("Failed to find network interface for [" + origHost + "]");
        }
        return InetAddress.getByName(host);
    }
}
