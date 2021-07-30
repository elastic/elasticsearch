/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cloud.gce.network;

import org.elasticsearch.cloud.gce.GceMetadataService;
import org.elasticsearch.cloud.gce.util.Access;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkService.CustomNameResolver;

import java.io.IOException;
import java.net.InetAddress;

/**
 * <p>Resolves certain GCE related 'meta' hostnames into an actual hostname
 * obtained from gce meta-data.</p>
 * Valid config values for {@link GceAddressResolverType}s are -
 * <ul>
 * <li>_gce_ - maps to privateIp</li>
 * <li>_gce:privateIp_</li>
 * <li>_gce:hostname_</li>
 * </ul>
 */
public class GceNameResolver implements CustomNameResolver {

    private final GceMetadataService gceMetadataService;

    /**
     * enum that can be added to over time with more meta-data types
     */
    private enum GceAddressResolverType {

        /**
         * Using the hostname
         */
        PRIVATE_DNS("gce:hostname", "hostname"),
        /**
         * Can be gce:privateIp, gce:privateIp:X where X is the network interface
         */
        PRIVATE_IP("gce:privateIp", "network-interfaces/{{network}}/ip"),
        /**
         * same as "gce:privateIp" or "gce:privateIp:0"
         */
        GCE("gce", PRIVATE_IP.gceName);

        final String configName;
        final String gceName;

        GceAddressResolverType(String configName, String gceName) {
            this.configName = configName;
            this.gceName = gceName;
        }
    }

    /**
     * Construct a {@link CustomNameResolver}.
     */
    public GceNameResolver(GceMetadataService gceMetadataService) {
        this.gceMetadataService = gceMetadataService;
    }

    /**
     * @param value the gce hostname type to discover.
     * @return the appropriate host resolved from gce meta-data.
     * @see CustomNameResolver#resolveIfPossible(String)
     */
    private InetAddress[] resolve(String value) throws IOException {
        String gceMetadataPath;
        if (value.equals(GceAddressResolverType.GCE.configName)) {
            // We replace network placeholder with default network interface value: 0
            gceMetadataPath = Strings.replace(GceAddressResolverType.GCE.gceName, "{{network}}", "0");
        } else if (value.equals(GceAddressResolverType.PRIVATE_DNS.configName)) {
            gceMetadataPath = GceAddressResolverType.PRIVATE_DNS.gceName;
        } else if (value.startsWith(GceAddressResolverType.PRIVATE_IP.configName)) {
            // We extract the network interface from gce:privateIp:XX
            String network = "0";
            String[] privateIpConfig = value.split(":");
            if (privateIpConfig.length == 3) {
                network = privateIpConfig[2];
            }

            // We replace network placeholder with network interface value
            gceMetadataPath = Strings.replace(GceAddressResolverType.PRIVATE_IP.gceName, "{{network}}", network);
        } else {
            throw new IllegalArgumentException("[" + value + "] is not one of the supported GCE network.host setting. " +
                    "Expecting _gce_, _gce:privateIp:X_, _gce:hostname_");
        }

        try {
            String metadataResult = Access.doPrivilegedIOException(() -> gceMetadataService.metadata(gceMetadataPath));
            if (metadataResult == null || metadataResult.length() == 0) {
                throw new IOException("no gce metadata returned from [" + gceMetadataPath + "] for [" + value + "]");
            }
            // only one address: because we explicitly ask for only one via the GceHostnameType
            return new InetAddress[] { InetAddress.getByName(metadataResult) };
        } catch (IOException e) {
            throw new IOException("IOException caught when fetching InetAddress from [" + gceMetadataPath + "]", e);
        }
    }

    @Override
    public InetAddress[] resolveDefault() {
        return null; // using this, one has to explicitly specify _gce_ in network setting
    }

    @Override
    public InetAddress[] resolveIfPossible(String value) throws IOException {
        // We only try to resolve network.host setting when it starts with _gce
        if (value.startsWith("gce")) {
            return resolve(value);
        }
        return null;
    }
}
