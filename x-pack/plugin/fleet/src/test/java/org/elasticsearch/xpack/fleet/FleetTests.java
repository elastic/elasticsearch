/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.fleet;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.test.ESTestCase;

import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class FleetTests extends ESTestCase {

    public void testFleetIndexNames() {
        Fleet module = new Fleet();

        assertThat(
            module.getSystemIndexDescriptors(Settings.EMPTY)
                .stream()
                .map(SystemIndexDescriptor::getIndexPattern)
                .collect(Collectors.toList()),
            containsInAnyOrder(".fleet-outputs*", ".fleet-policies*", ".fleet-agents*")
        );

        assertTrue(module.getSystemIndexDescriptors(Settings.EMPTY).stream().anyMatch(d -> d.matchesIndexPattern(".fleet-outputs")));

        assertTrue(module.getSystemIndexDescriptors(Settings.EMPTY).stream().anyMatch(d -> d.matchesIndexPattern(".fleet-policies")));
        assertTrue(
            module.getSystemIndexDescriptors(Settings.EMPTY)
                .stream()
                .anyMatch(d -> d.matchesIndexPattern(".fleet-policies-enrollment-keys"))
        );
        assertTrue(
            module.getSystemIndexDescriptors(Settings.EMPTY).stream().anyMatch(d -> d.matchesIndexPattern(".fleet-policies-inputs"))
        );

        assertTrue(module.getSystemIndexDescriptors(Settings.EMPTY).stream().anyMatch(d -> d.matchesIndexPattern(".fleet-agents")));
        assertTrue(
            module.getSystemIndexDescriptors(Settings.EMPTY).stream().anyMatch(d -> d.matchesIndexPattern(".fleet-agents-checkins"))
        );

    }
}
