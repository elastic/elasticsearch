/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Map;

public class HostMetadataTests extends ESTestCase {
    public void testCreateFromSourceAWS() {
        final String hostID = "1440256254710195396";
        final String arch = "amd64";
        final String provider = "aws";
        final String region = "eu-west-1";
        final String instanceType = "md5x.large";

        // tag::noformat
        HostMetadata host = HostMetadata.fromSource (
            Map.of (
                "host.id", hostID,
                "host.arch", arch,
                "host.type", instanceType,
                "cloud.provider", provider,
                "cloud.region", region
            )
        );
        // end::noformat

        assertEquals(hostID, host.hostID);
        assertEquals(arch, host.hostArchitecture);
        assertEquals(provider, host.instanceType.provider);
        assertEquals(region, host.instanceType.region);
        assertEquals(instanceType, host.instanceType.name);
    }

    public void testCreateFromSourceAWSCompat() {
        final String hostID = "1440256254710195396";
        final String arch = "x86_64";
        final String provider = "aws";
        final String region = "eu-west-1";
        final String instanceType = "md5x.large";

        // tag::noformat
        HostMetadata host = HostMetadata.fromSource (
            Map.of (
                "host.id", hostID,
                "host.arch", arch,
                "ec2.instance_type", instanceType,
                "ec2.placement.region", region
            )
        );
        // end::noformat

        assertEquals(hostID, host.hostID);
        assertEquals(arch, host.hostArchitecture);
        assertEquals(provider, host.instanceType.provider);
        assertEquals(region, host.instanceType.region);
        assertEquals(instanceType, host.instanceType.name);
    }

    public void testCreateFromSourceGCP() {
        final String hostID = "1440256254710195396";
        final String arch = "amd64";
        final String provider = "gcp";
        final String[] regions = { "", "", "europe-west1", "europewest", "europe-west1" };

        for (String region : regions) {
            // tag::noformat
            HostMetadata host = HostMetadata.fromSource (
                Map.of (
                    "host.id", hostID,
                    "host.arch", arch,
                    "cloud.provider", provider,
                    "cloud.region", region
                )
            );
            // end::noformat

            assertEquals(hostID, host.hostID);
            assertEquals(arch, host.hostArchitecture);
            assertEquals(provider, host.instanceType.provider);
            assertEquals(region, host.instanceType.region);
            assertEquals("", host.instanceType.name);
        }
    }

    public void testCreateFromSourceGCPCompat() {
        final String hostID = "1440256254710195396";
        final String arch = "x86_64";
        final String provider = "gcp";
        final String[] regions = { "", "", "europe-west1", "europewest", "europe-west1" };
        final String[] zones = {
            "",
            "/",
            "projects/123456789/zones/" + regions[2] + "-b",
            "projects/123456789/zones/" + regions[3],
            "projects/123456789/zones/" + regions[4] + "-b-c" };

        for (int i = 0; i < regions.length; i++) {
            String region = regions[i];
            String zone = zones[i];

            // tag::noformat
            HostMetadata host = HostMetadata.fromSource(
                Map.of(
                    "host.id", hostID,
                    "host.arch", arch,
                    "gce.instance.zone", zone
                )
            );
            // end::noformat

            assertEquals(hostID, host.hostID);
            assertEquals(arch, host.hostArchitecture);
            assertEquals(provider, host.instanceType.provider);
            assertEquals(region, host.instanceType.region);
            assertEquals("", host.instanceType.name);
        }
    }

    public void testCreateFromSourceGCPZoneFuzzer() {
        final String hostID = "1440256254710195396";
        final String arch = "x86_64";
        final String provider = "gcp";
        final Character[] chars = new Character[] { '/', '-', 'a' };

        for (int zoneLength = 1; zoneLength <= 5; zoneLength++) {
            CarthesianCombinator<Character> combinator = new CarthesianCombinator<>(chars, zoneLength);

            combinator.forEach((result) -> {
                StringBuilder sb = new StringBuilder();
                for (Character c : result) {
                    sb.append(c);
                }
                String zone = sb.toString();

                // tag::noformat
                HostMetadata host = HostMetadata.fromSource(
                    Map.of(
                        "host.id", hostID,
                        "host.arch", arch,
                        "gce.instance.zone", zone
                    )
                );
                // end::noformat

                assertEquals(hostID, host.hostID);
                assertEquals(arch, host.hostArchitecture);
                assertEquals(provider, host.instanceType.provider);
                assertNotNull(host.instanceType.region);
                assertEquals("", host.instanceType.name);
                // region isn't tested because of the combinatorial nature of this test
            });
        }
    }

    public void testCreateFromSourceAzure() {
        final String hostID = "1440256254710195396";
        final String arch = "amd64";
        final String provider = "azure";
        final String region = "eastus2";

        // tag::noformat
        HostMetadata host = HostMetadata.fromSource(
            Map.of(
                "host.id", hostID,
                "host.arch", arch,
                "azure.compute.location", region
            )
        );
        // end::noformat

        assertEquals(hostID, host.hostID);
        assertEquals(arch, host.hostArchitecture);
        assertEquals(provider, host.instanceType.provider);
        assertEquals(region, host.instanceType.region);
        assertEquals("", host.instanceType.name);
    }

    public void testCreateFromSourceECS() {
        final String hostID = "1440256254710195396";
        final String arch = "amd64";
        final String provider = "any-provider";
        final String region = "any-region";

        // tag::noformat
        HostMetadata host = HostMetadata.fromSource(
            Map.of(
                "host.id", hostID,
                "host.arch", arch,
                "profiling.host.tags", Arrays.asList (
                    "cloud_provider:" + provider, "cloud_environment:qa", "cloud_region:" + region)
            )
        );
        // end::noformat

        assertEquals(hostID, host.hostID);
        assertEquals(arch, host.hostArchitecture);
        assertEquals(provider, host.instanceType.provider);
        assertEquals(region, host.instanceType.region);
        assertEquals("", host.instanceType.name);
    }

    public void testCreateFromSourceNoProvider() {
        final String hostID = "1440256254710195396";
        final String arch = "amd64";

        // tag::noformat
        HostMetadata host = HostMetadata.fromSource(
            Map.of(
                "host.id", hostID,
                "host.arch", arch
            )
        );
        // end::noformat

        assertEquals(hostID, host.hostID);
        assertEquals(arch, host.hostArchitecture);
        assertEquals("", host.instanceType.provider);
        assertEquals("", host.instanceType.region);
        assertEquals("", host.instanceType.name);
    }

    public void testCreateFromSourceArchitectureFallback() {
        final String hostID = "1440256254710195396";
        final String machine = "x86_64";

        // tag::noformat
        HostMetadata host = HostMetadata.fromSource(
            // Missing host.arch field, pre-8.14.0 architecture value
            Map.of(
                "host.id", hostID,
                "profiling.host.machine", machine
            )
        );
        // end::noformat

        assertEquals(hostID, host.hostID);
        assertEquals(machine, host.hostArchitecture);
        assertEquals("", host.instanceType.provider);
        assertEquals("", host.instanceType.region);
        assertEquals("", host.instanceType.name);
    }
}
