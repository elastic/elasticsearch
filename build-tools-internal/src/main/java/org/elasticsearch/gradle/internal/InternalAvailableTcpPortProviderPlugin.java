/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.internal.util.ports.AvailablePortAllocator;
import org.elasticsearch.gradle.internal.util.ports.ReservedPortRange;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.provider.Provider;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;

/**
 * Reserves a distinct TCP port range for each project that applies this plugin and exposes it as the {@code portRange} extension.
 * <p>
 * The ranges are handed out by a single {@link AvailablePortAllocator} that is shared through a per-build
 * {@link AvailablePortAllocatorService}, so ranges do not overlap across the projects of a build without any project
 * having to reach into the root project.
 */
public class InternalAvailableTcpPortProviderPlugin implements Plugin<Project> {

    static final String AVAILABLE_PORT_ALLOCATOR_SERVICE_NAME = "elasticsearchAvailablePortAllocator";

    @Override
    public void apply(Project project) {
        Provider<AvailablePortAllocatorService> allocatorService = project.getGradle()
            .getSharedServices()
            .registerIfAbsent(AVAILABLE_PORT_ALLOCATOR_SERVICE_NAME, AvailablePortAllocatorService.class, spec -> {});
        ReservedPortRange portRange = allocatorService.get().reservePortRange();
        project.getExtensions().add("portRange", portRange);
    }

    /**
     * Holds the build-wide {@link AvailablePortAllocator}. Projects may be configured in parallel, so reservations are synchronized.
     */
    public abstract static class AvailablePortAllocatorService implements BuildService<BuildServiceParameters.None> {
        private final AvailablePortAllocator allocator = new AvailablePortAllocator();

        public synchronized ReservedPortRange reservePortRange() {
            return allocator.reservePortRange();
        }
    }
}
