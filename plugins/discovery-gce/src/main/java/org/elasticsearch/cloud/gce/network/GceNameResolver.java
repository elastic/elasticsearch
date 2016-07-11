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

package org.elasticsearch.cloud.gce.network;

import org.elasticsearch.cloud.gce.GceComputeService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.network.NetworkService.CustomNameResolver;
import org.elasticsearch.common.settings.Settings;

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
public class GceNameResolver extends AbstractComponent implements CustomNameResolver {

    private final GceComputeService gceComputeService;

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
    public GceNameResolver(Settings settings, GceComputeService gceComputeService) {
        super(settings);
        this.gceComputeService = gceComputeService;
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
            if (privateIpConfig != null && privateIpConfig.length == 3) {
                network = privateIpConfig[2];
            }

            // We replace network placeholder with network interface value
            gceMetadataPath = Strings.replace(GceAddressResolverType.PRIVATE_IP.gceName, "{{network}}", network);
        } else {
            throw new IllegalArgumentException("[" + value + "] is not one of the supported GCE network.host setting. " +
                    "Expecting _gce_, _gce:privateIp:X_, _gce:hostname_");
        }

        try {
            String metadataResult = gceComputeService.metadata(gceMetadataPath);
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
