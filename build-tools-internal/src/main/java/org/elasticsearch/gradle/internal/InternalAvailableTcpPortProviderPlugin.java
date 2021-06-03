/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.internal.util.ports.AvailablePortAllocator;
import org.elasticsearch.gradle.internal.util.ports.ReservedPortRange;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

public class InternalAvailableTcpPortProviderPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        AvailablePortAllocator allocator = project.getRootProject()
            .getPlugins()
            .apply(InternalAvailableTcpPortProviderRootPlugin.class).allocator;
        ReservedPortRange portRange = allocator.reservePortRange();
        project.getExtensions().add("portRange", portRange);
    }

    static class InternalAvailableTcpPortProviderRootPlugin implements Plugin<Project> {
        AvailablePortAllocator allocator;

        @Override
        public void apply(Project project) {
            allocator = new AvailablePortAllocator();
        }
    }
}
