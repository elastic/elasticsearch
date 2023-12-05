/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Map;

public class HostMetadataTests extends ESTestCase {
    public void testCreateFromRegularSource() {
        final String hostID = "1440256254710195396";
        final String machine = "x86_64";
        final String provider = "aws";
        final String region = "eu-west-1";
        final String instanceType = "md5x.large";

        // tag::noformat
        HostMetadata host = HostMetadata.fromSource(
            Map.of(
                "host.id", hostID,
                "profiling.host.machine", machine,
                "profiling.host.tags", Arrays.asList(
                    "cloud_provider:"+provider, "cloud_environment:qa", "cloud_region:"+region),
                "ec2.instance_type", instanceType
            )
        );
        // end::noformat

        assertEquals(hostID, host.hostID);
        assertEquals(machine, host.profilingHostMachine);
        assertEquals(provider, host.instanceType.provider);
        assertEquals(region, host.instanceType.region);
        assertEquals(instanceType, host.instanceType.name);
    }
}
