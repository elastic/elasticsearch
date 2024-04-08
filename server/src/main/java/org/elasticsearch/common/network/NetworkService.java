/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.network;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public final class NetworkService {

    /** By default, we bind to loopback interfaces */
    public static final String DEFAULT_NETWORK_HOST = "_local_";
    public static final Setting<Boolean> NETWORK_SERVER = Setting.boolSetting("network.server", true, Property.NodeScope);
    public static final Setting<List<String>> GLOBAL_NETWORK_HOST_SETTING = Setting.stringListSetting("network.host", Property.NodeScope);
    public static final Setting<List<String>> GLOBAL_NETWORK_BIND_HOST_SETTING = Setting.listSetting(
        "network.bind_host",
        GLOBAL_NETWORK_HOST_SETTING,
        Function.identity(),
        Property.NodeScope
    );
    public static final Setting<List<String>> GLOBAL_NETWORK_PUBLISH_HOST_SETTING = Setting.listSetting(
        "network.publish_host",
        GLOBAL_NETWORK_HOST_SETTING,
        Function.identity(),
        Property.NodeScope
    );

    public static final Setting<Boolean> TCP_NO_DELAY = Setting.boolSetting("network.tcp.no_delay", true, Property.NodeScope);
    public static final Setting<Boolean> TCP_KEEP_ALIVE = Setting.boolSetting("network.tcp.keep_alive", true, Property.NodeScope);
    public static final Setting<Integer> TCP_KEEP_IDLE = Setting.intSetting("network.tcp.keep_idle", -1, -1, 300, Property.NodeScope);
    public static final Setting<Integer> TCP_KEEP_INTERVAL = Setting.intSetting(
        "network.tcp.keep_interval",
        -1,
        -1,
        300,
        Property.NodeScope
    );
    public static final Setting<Integer> TCP_KEEP_COUNT = Setting.intSetting("network.tcp.keep_count", -1, -1, Property.NodeScope);
    public static final Setting<Boolean> TCP_REUSE_ADDRESS = Setting.boolSetting(
        "network.tcp.reuse_address",
        NetworkUtils.defaultReuseAddress(),
        Property.NodeScope
    );
    public static final Setting<ByteSizeValue> TCP_SEND_BUFFER_SIZE = Setting.byteSizeSetting(
        "network.tcp.send_buffer_size",
        ByteSizeValue.MINUS_ONE,
        Property.NodeScope
    );
    public static final Setting<ByteSizeValue> TCP_RECEIVE_BUFFER_SIZE = Setting.byteSizeSetting(
        "network.tcp.receive_buffer_size",
        ByteSizeValue.MINUS_ONE,
        Property.NodeScope
    );

    /**
     * A custom name resolver can support custom lookup keys (my_net_key:ipv4) and also change
     * the default inet address used in case no settings is provided.
     */
    public interface CustomNameResolver {
        /**
         * Resolves the default value if possible. If not, return {@code null}.
         */
        InetAddress[] resolveDefault();

        /**
         * Resolves a custom value handling, return {@code null} if can't handle it.
         */
        InetAddress[] resolveIfPossible(String value) throws IOException;
    }

    private final List<CustomNameResolver> customNameResolvers;
    private final HandlingTimeTracker handlingTimeTracker = new HandlingTimeTracker();

    public NetworkService(List<CustomNameResolver> customNameResolvers) {
        this.customNameResolvers = Objects.requireNonNull(customNameResolvers, "customNameResolvers must be non null");
    }

    public HandlingTimeTracker getHandlingTimeTracker() {
        return handlingTimeTracker;
    }

    /**
     * Resolves {@code bindHosts} to a list of internet addresses. The list will
     * not contain duplicate addresses.
     *
     * @param bindHosts list of hosts to bind to. this may contain special pseudo-hostnames
     *                  such as _local_ (see the documentation). if it is null, it will fall back to _local_
     *
     * @return unique set of internet addresses
     */
    public InetAddress[] resolveBindHostAddresses(String bindHosts[]) throws IOException {
        if (bindHosts == null || bindHosts.length == 0) {
            for (CustomNameResolver customNameResolver : customNameResolvers) {
                InetAddress addresses[] = customNameResolver.resolveDefault();
                if (addresses != null) {
                    return addresses;
                }
            }
            // we know it's not here. get the defaults
            bindHosts = new String[] { "_local_" };
        }

        InetAddress addresses[] = resolveInetAddresses(bindHosts);

        // try to deal with some (mis)configuration
        for (InetAddress address : addresses) {
            // check if its multicast: flat out mistake
            if (address.isMulticastAddress()) {
                throw new IllegalArgumentException("bind address: {" + NetworkAddress.format(address) + "} is invalid: multicast address");
            }
            // check if its a wildcard address: this is only ok if its the only address!
            if (address.isAnyLocalAddress() && addresses.length > 1) {
                throw new IllegalArgumentException(
                    "bind address: {"
                        + NetworkAddress.format(address)
                        + "} is wildcard, but multiple addresses specified: this makes no sense"
                );
            }
        }
        return addresses;
    }

    /**
     * Resolves {@code publishHosts} to a single publish address. The fact that it returns
     * only one address is just a current limitation.
     * <p>
     * If {@code publishHosts} resolves to more than one address, <b>then one is selected with magic</b>
     *
     * @param publishHosts list of hosts to publish as. this may contain special pseudo-hostnames
     *                     such as _local_ (see the documentation). if it is null, it will fall back to _local_
     * @return single internet address
     */
    // TODO: needs to be InetAddress[]
    public InetAddress resolvePublishHostAddresses(String publishHosts[]) throws IOException {
        if (publishHosts == null || publishHosts.length == 0) {
            for (CustomNameResolver customNameResolver : customNameResolvers) {
                InetAddress addresses[] = customNameResolver.resolveDefault();
                if (addresses != null) {
                    return addresses[0];
                }
            }
            // we know it's not here. get the defaults
            publishHosts = new String[] { DEFAULT_NETWORK_HOST };
        }

        InetAddress addresses[] = resolveInetAddresses(publishHosts);
        // TODO: allow publishing multiple addresses
        // for now... the hack begins

        // 1. single wildcard address, probably set by network.host: expand to all interface addresses.
        if (addresses.length == 1 && addresses[0].isAnyLocalAddress()) {
            HashSet<InetAddress> all = new HashSet<>(Arrays.asList(NetworkUtils.getAllAddresses()));
            addresses = all.toArray(new InetAddress[all.size()]);
        }

        // 2. try to deal with some (mis)configuration
        for (InetAddress address : addresses) {
            // check if its multicast: flat out mistake
            if (address.isMulticastAddress()) {
                throw new IllegalArgumentException(
                    "publish address: {" + NetworkAddress.format(address) + "} is invalid: multicast address"
                );
            }
            // check if its a wildcard address: this is only ok if its the only address!
            // (if it was a single wildcard address, it was replaced by step 1 above)
            if (address.isAnyLocalAddress()) {
                throw new IllegalArgumentException(
                    "publish address: {"
                        + NetworkAddress.format(address)
                        + "} is wildcard, but multiple addresses specified: this makes no sense"
                );
            }
        }

        // 3. if we end out with multiple publish addresses, select by preference.
        // don't warn the user, or they will get confused by bind_host vs publish_host etc.
        if (addresses.length > 1) {
            List<InetAddress> sorted = new ArrayList<>(Arrays.asList(addresses));
            NetworkUtils.sortAddresses(sorted);
            addresses = new InetAddress[] { sorted.get(0) };
        }
        return addresses[0];
    }

    /** resolves (and deduplicates) host specification */
    private InetAddress[] resolveInetAddresses(String hosts[]) throws IOException {
        if (hosts.length == 0) {
            throw new IllegalArgumentException("empty host specification");
        }
        // deduplicate, in case of resolver misconfiguration
        // stuff like https://bugzilla.redhat.com/show_bug.cgi?id=496300
        HashSet<InetAddress> set = new HashSet<>();
        for (String host : hosts) {
            set.addAll(Arrays.asList(resolveInternal(host)));
        }
        return set.toArray(new InetAddress[set.size()]);
    }

    /** resolves a single host specification */
    private InetAddress[] resolveInternal(final String host) throws IOException {
        if ((host.startsWith("#") && host.endsWith("#")) || (host.startsWith("_") && host.endsWith("_"))) {
            final String interfaceSpec = host.substring(1, host.length() - 1);
            // next check any registered custom resolvers if any
            if (customNameResolvers != null) {
                for (CustomNameResolver customNameResolver : customNameResolvers) {
                    InetAddress addresses[] = customNameResolver.resolveIfPossible(interfaceSpec);
                    if (addresses != null) {
                        return addresses;
                    }
                }
            }
            switch (interfaceSpec) {
                case "local":
                    return NetworkUtils.getLoopbackAddresses();
                case "local:ipv4":
                    return NetworkUtils.filterIPV4(NetworkUtils.getLoopbackAddresses());
                case "local:ipv6":
                    return NetworkUtils.filterIPV6(NetworkUtils.getLoopbackAddresses());
                case "site":
                    return NetworkUtils.getSiteLocalAddresses();
                case "site:ipv4":
                    return NetworkUtils.filterIPV4(NetworkUtils.getSiteLocalAddresses());
                case "site:ipv6":
                    return NetworkUtils.filterIPV6(NetworkUtils.getSiteLocalAddresses());
                case "global":
                    return NetworkUtils.getGlobalAddresses();
                case "global:ipv4":
                    return NetworkUtils.filterIPV4(NetworkUtils.getGlobalAddresses());
                case "global:ipv6":
                    return NetworkUtils.filterIPV6(NetworkUtils.getGlobalAddresses());
                default:
                    /* an interface specification */
                    if (interfaceSpec.endsWith(":ipv4")) {
                        return NetworkUtils.filterIPV4(
                            NetworkUtils.getAddressesForInterface(host, ":ipv4", interfaceSpec.substring(0, interfaceSpec.length() - 5))
                        );
                    } else if (interfaceSpec.endsWith(":ipv6")) {
                        return NetworkUtils.filterIPV6(
                            NetworkUtils.getAddressesForInterface(host, ":ipv6", interfaceSpec.substring(0, interfaceSpec.length() - 5))
                        );
                    } else {
                        return NetworkUtils.getAddressesForInterface(host, "", interfaceSpec);
                    }
            }
        }
        return InetAddress.getAllByName(host);
    }
}
