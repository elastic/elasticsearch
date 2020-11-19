/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.fleet;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.test.ESTestCase;

import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class FleetModuleTests extends ESTestCase {

    public void testFleetIndexNames() {
        FleetModule module = new FleetModule();

        assertThat(
            module.getSystemIndexDescriptors(Settings.EMPTY).stream()
                .map(SystemIndexDescriptor::getIndexPattern)
                .collect(Collectors.toList()),
            containsInAnyOrder(".fleet-outputs*", ".fleet-policies*", ".fleet-agents*"));

        assertTrue(module.getSystemIndexDescriptors(Settings.EMPTY).stream().anyMatch(d -> d.matchesIndexPattern(".fleet-outputs")));

        assertTrue(module.getSystemIndexDescriptors(Settings.EMPTY).stream().anyMatch(d -> d.matchesIndexPattern(".fleet-policies")));
        assertTrue(module.getSystemIndexDescriptors(Settings.EMPTY).stream()
            .anyMatch(d -> d.matchesIndexPattern(".fleet-policies-enrollment-keys")));
        assertTrue(module.getSystemIndexDescriptors(Settings.EMPTY).stream()
            .anyMatch(d -> d.matchesIndexPattern(".fleet-policies-inputs")));

        assertTrue(module.getSystemIndexDescriptors(Settings.EMPTY).stream().anyMatch(d -> d.matchesIndexPattern(".fleet-agents")));
        assertTrue(module.getSystemIndexDescriptors(Settings.EMPTY).stream()
            .anyMatch(d -> d.matchesIndexPattern(".fleet-agents-checkins")));

    }
}
