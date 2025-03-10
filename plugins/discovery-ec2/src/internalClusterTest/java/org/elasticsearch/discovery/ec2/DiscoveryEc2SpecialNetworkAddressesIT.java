/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery.ec2;

import fixture.aws.imds.Ec2ImdsHttpFixture;
import fixture.aws.imds.Ec2ImdsServiceBuilder;
import fixture.aws.imds.Ec2ImdsVersion;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import java.util.Map;

/**
 * Verifies that the special network addresses {@code _ec2:*_} work by retrieving information from the IMDS. See {@code discovery-ec2}
 * plugin docs for more information.
 */
public class DiscoveryEc2SpecialNetworkAddressesIT extends DiscoveryEc2NetworkAddressesTestCase {

    private final String imdsAddressName;
    private final String elasticsearchAddressName;

    public DiscoveryEc2SpecialNetworkAddressesIT(
        @Name("imdsAddressName") String imdsAddressName,
        @Name("elasticsearchAddressName") String elasticsearchAddressName
    ) {
        this.imdsAddressName = imdsAddressName;
        this.elasticsearchAddressName = elasticsearchAddressName;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return Map.of(
            "_ec2:privateIpv4_",
            "local-ipv4",
            "_ec2:privateDns_",
            "local-hostname",
            "_ec2:publicIpv4_",
            "public-ipv4",
            "_ec2:publicDns_",
            "public-hostname",
            "_ec2:publicIp_",
            "public-ipv4",
            "_ec2:privateIp_",
            "local-ipv4",
            "_ec2_",
            "local-ipv4"
        ).entrySet().stream().map(addresses -> new Object[] { addresses.getValue(), addresses.getKey() }).toList();
    }

    public void testSpecialNetworkAddresses() {
        final var publishAddress = "10.0." + between(0, 255) + "." + between(0, 255);
        Ec2ImdsHttpFixture.runWithFixture(
            new Ec2ImdsServiceBuilder(Ec2ImdsVersion.V2).addInstanceAddress(imdsAddressName, publishAddress),
            imdsFixture -> {
                try (var ignored = Ec2ImdsHttpFixture.withEc2MetadataServiceEndpointOverride(imdsFixture.getAddress())) {
                    verifyPublishAddress(elasticsearchAddressName, publishAddress);
                }
            }
        );
    }

}
