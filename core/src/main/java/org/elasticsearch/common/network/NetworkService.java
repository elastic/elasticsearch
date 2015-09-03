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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class NetworkService extends AbstractComponent {

    /** By default, we bind to loopback interfaces */
    public static final String DEFAULT_NETWORK_HOST = "_local_";

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
        InetAddress[] resolveDefault();

        /**
         * Resolves a custom value handling, return <tt>null</tt> if can't handle it.
         */
        InetAddress[] resolveIfPossible(String value);
    }

    private final List<CustomNameResolver> customNameResolvers = new CopyOnWriteArrayList<>();

    @Inject
    public NetworkService(Settings settings) {
        super(settings);
        IfConfig.logIfNecessary();
    }

    /**
     * Add a custom name resolver.
     */
    public void addCustomNameResolver(CustomNameResolver customNameResolver) {
        customNameResolvers.add(customNameResolver);
    }

    public InetAddress[] resolveBindHostAddress(String bindHost) throws IOException {
        // first check settings
        if (bindHost == null) {
            bindHost = settings.get(GLOBAL_NETWORK_BINDHOST_SETTING, settings.get(GLOBAL_NETWORK_HOST_SETTING));
        }
        // next check any registered custom resolvers
        if (bindHost == null) {
            for (CustomNameResolver customNameResolver : customNameResolvers) {
                InetAddress addresses[] = customNameResolver.resolveDefault();
                if (addresses != null) {
                    return addresses;
                }
            }
        }
        // finally, fill with our default
        if (bindHost == null) {
            bindHost = DEFAULT_NETWORK_HOST;
        }
        InetAddress addresses[] = resolveInetAddress(bindHost);

        // try to deal with some (mis)configuration
        if (addresses != null) {
            for (InetAddress address : addresses) {
                // check if its multicast: flat out mistake
                if (address.isMulticastAddress()) {
                    throw new IllegalArgumentException("bind address: {" + NetworkAddress.format(address) + "} is invalid: multicast address");
                }
            }
        }
        return addresses;
    }

    // TODO: needs to be InetAddress[]
    public InetAddress resolvePublishHostAddress(String publishHost) throws IOException {
        // first check settings
        if (publishHost == null) {
            publishHost = settings.get(GLOBAL_NETWORK_PUBLISHHOST_SETTING, settings.get(GLOBAL_NETWORK_HOST_SETTING));
        }
        // next check any registered custom resolvers
        if (publishHost == null) {
            for (CustomNameResolver customNameResolver : customNameResolvers) {
                InetAddress addresses[] = customNameResolver.resolveDefault();
                if (addresses != null) {
                    return addresses[0];
                }
            }
        }
        // finally, fill with our default
        if (publishHost == null) {
            publishHost = DEFAULT_NETWORK_HOST;
        }
        // TODO: allow publishing multiple addresses
        InetAddress address = resolveInetAddress(publishHost)[0];

        // try to deal with some (mis)configuration
        if (address != null) {
            // check if its multicast: flat out mistake
            if (address.isMulticastAddress()) {
                throw new IllegalArgumentException("publish address: {" + NetworkAddress.format(address) + "} is invalid: multicast address");
            }
            // wildcard address, probably set by network.host
            if (address.isAnyLocalAddress()) {
                InetAddress old = address;
                address = NetworkUtils.getFirstNonLoopbackAddresses()[0];
                logger.warn("publish address: {{}} is a wildcard address, falling back to first non-loopback: {{}}", 
                            NetworkAddress.format(old), NetworkAddress.format(address));
            }
        }
        return address;
    }

    private InetAddress[] resolveInetAddress(String host) throws UnknownHostException, IOException {
        if ((host.startsWith("#") && host.endsWith("#")) || (host.startsWith("_") && host.endsWith("_"))) {
            host = host.substring(1, host.length() - 1);
            // allow custom resolvers to have special names
            for (CustomNameResolver customNameResolver : customNameResolvers) {
                InetAddress addresses[] = customNameResolver.resolveIfPossible(host);
                if (addresses != null) {
                    return addresses;
                }
            }
            switch (host) {
                case "local":
                    return NetworkUtils.getLoopbackAddresses();
                case "local:ipv4":
                    return NetworkUtils.filterIPV4(NetworkUtils.getLoopbackAddresses());
                case "local:ipv6":
                    return NetworkUtils.filterIPV6(NetworkUtils.getLoopbackAddresses());
                case "non_loopback":
                    return NetworkUtils.getFirstNonLoopbackAddresses();
                case "non_loopback:ipv4":
                    return NetworkUtils.filterIPV4(NetworkUtils.getFirstNonLoopbackAddresses());
                case "non_loopback:ipv6":
                    return NetworkUtils.filterIPV6(NetworkUtils.getFirstNonLoopbackAddresses());
                default:
                    /* an interface specification */
                    if (host.endsWith(":ipv4")) {
                        host = host.substring(0, host.length() - 5);
                        return NetworkUtils.filterIPV4(NetworkUtils.getAddressesForInterface(host));
                    } else if (host.endsWith(":ipv6")) {
                        host = host.substring(0, host.length() - 5);
                        return NetworkUtils.filterIPV6(NetworkUtils.getAddressesForInterface(host));
                    } else {
                        return NetworkUtils.getAddressesForInterface(host);
                    }
            }
        }
        return NetworkUtils.getAllByName(host);
    }
}
