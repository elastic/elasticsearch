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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.BindTransportException;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;

public class DiscoveryEc2RegularNetworkAddressesIT extends DiscoveryEc2NetworkAddressesTestCase {
    public void testLocalIgnoresImds() {
        Ec2ImdsHttpFixture.runWithFixture(new Ec2ImdsServiceBuilder(randomFrom(Ec2ImdsVersion.values())), imdsFixture -> {
            try (var ignored = Ec2ImdsHttpFixture.withEc2MetadataServiceEndpointOverride(imdsFixture.getAddress())) {
                verifyPublishAddress("_local_", "127.0.0.1");
            }
        });
    }

    public void testImdsNotAvailable() throws IOException {
        try (var ignored = Ec2ImdsHttpFixture.withEc2MetadataServiceEndpointOverride("http://127.0.0.1")) {
            // if IMDS is not running, regular values like `_local_` should still work
            verifyPublishAddress("_local_", "127.0.0.1");

            // but EC2 addresses will cause the node to fail to start
            final var assertionError = expectThrows(
                AssertionError.class,
                () -> internalCluster().startNode(Settings.builder().put("http.publish_host", "_ec2_"))
            );
            final var executionException = asInstanceOf(ExecutionException.class, assertionError.getCause());
            final var bindTransportException = asInstanceOf(BindTransportException.class, executionException.getCause());
            assertEquals("Failed to resolve publish address", bindTransportException.getMessage());
            final var ioException = asInstanceOf(IOException.class, bindTransportException.getCause());
            assertThat(ioException.getMessage(), containsString("/latest/meta-data/local-ipv4"));
        }
    }
}
