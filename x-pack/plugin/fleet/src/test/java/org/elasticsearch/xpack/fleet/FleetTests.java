/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.SystemIndices.Feature;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.test.ESTestCase;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class FleetTests extends ESTestCase {
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors() {
        SystemIndexPlugin plugin = new Fleet();
        return plugin.getSystemIndexDescriptors(Settings.EMPTY);
    }

    public void testSystemIndexDescriptorFormats() {
        for (SystemIndexDescriptor descriptor : getSystemIndexDescriptors()) {
            assertTrue(descriptor.isAutomaticallyManaged());
        }
    }

    public void testFleetIndexNames() {
        final Collection<SystemIndexDescriptor> fleetDescriptors = getSystemIndexDescriptors();

        assertThat(
            fleetDescriptors.stream().map(SystemIndexDescriptor::getIndexPattern).collect(Collectors.toList()),
            containsInAnyOrder(
                ".fleet-servers*",
                ".fleet-policies-[0-9]+*",
                ".fleet-agents*",
                ".fleet-actions~(-results*)",
                ".fleet-policies-leader*",
                ".fleet-enrollment-api-keys*",
                ".fleet-artifacts*",
                ".fleet-secrets*"
            )
        );

        assertTrue(fleetDescriptors.stream().anyMatch(d -> d.matchesIndexPattern(".fleet-servers")));

        assertTrue(fleetDescriptors.stream().anyMatch(d -> d.matchesIndexPattern(".fleet-policies")));
        assertTrue(fleetDescriptors.stream().anyMatch(d -> d.matchesIndexPattern(".fleet-policies-leader")));

        assertTrue(fleetDescriptors.stream().anyMatch(d -> d.matchesIndexPattern(".fleet-agents")));

        assertTrue(fleetDescriptors.stream().anyMatch(d -> d.matchesIndexPattern(".fleet-actions")));
        assertFalse(fleetDescriptors.stream().anyMatch(d -> d.matchesIndexPattern(".fleet-actions-results")));

        assertTrue(fleetDescriptors.stream().anyMatch(d -> d.matchesIndexPattern(".fleet-secrets")));
    }

    public void testFleetFeature() {
        Fleet module = new Fleet();
        Feature fleet = Feature.fromSystemIndexPlugin(module, Settings.EMPTY);
        SystemIndices systemIndices = new SystemIndices(List.of(fleet));
        assertNotNull(systemIndices);
    }
}
