/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery.ec2;

import org.elasticsearch.common.network.NetworkService.CustomNameResolver;

import java.io.IOException;
import java.net.InetAddress;

/**
 * Resolves certain EC2 related 'meta' hostnames into an actual hostname
 * obtained from the EC2 instance metadata service
 * <p>
 * Valid config values for {@link Ec2HostnameType}s are -
 * <ul>
 * <li>{@code _ec2_} (maps to privateIpv4)</li>
 * <li>{@code _ec2:privateIp_} (maps to privateIpv4)</li>
 * <li>{@code _ec2:privateIpv4_}</li>
 * <li>{@code _ec2:privateDns_}</li>
 * <li>{@code _ec2:publicIp_} (maps to publicIpv4)</li>
 * <li>{@code _ec2:publicIpv4_}</li>
 * <li>{@code _ec2:publicDns_}</li>
 * </ul>
 */
class Ec2NameResolver implements CustomNameResolver {

    private enum Ec2HostnameType {

        PRIVATE_IPv4("ec2:privateIpv4", "local-ipv4"),
        PRIVATE_DNS("ec2:privateDns", "local-hostname"),
        PUBLIC_IPv4("ec2:publicIpv4", "public-ipv4"),
        PUBLIC_DNS("ec2:publicDns", "public-hostname"),

        // some less verbose defaults
        PUBLIC_IP("ec2:publicIp", PUBLIC_IPv4.ec2Name),
        PRIVATE_IP("ec2:privateIp", PRIVATE_IPv4.ec2Name),
        EC2("ec2", PRIVATE_IPv4.ec2Name);

        final String configName;
        final String ec2Name;

        Ec2HostnameType(String configName, String ec2Name) {
            this.configName = configName;
            this.ec2Name = ec2Name;
        }
    }

    @Override
    public InetAddress[] resolveDefault() {
        return null; // using this, one has to explicitly specify _ec2_ in network setting
    }

    private static final String IMDS_ADDRESS_PATH_PREFIX = "/latest/meta-data/";

    @Override
    public InetAddress[] resolveIfPossible(String value) throws IOException {
        for (Ec2HostnameType type : Ec2HostnameType.values()) {
            if (type.configName.equals(value)) {
                final var metadataPath = IMDS_ADDRESS_PATH_PREFIX + type.ec2Name;
                try {
                    // only one address: IMDS returns just one address/name, and if it's a name then it should resolve to one address
                    return new InetAddress[] { InetAddress.getByName(AwsEc2Utils.getInstanceMetadata(metadataPath)) };
                } catch (Exception e) {
                    throw new IOException("Exception caught when resolving EC2 address from [" + metadataPath + "]", e);
                }
            }
        }
        return null;
    }

}
