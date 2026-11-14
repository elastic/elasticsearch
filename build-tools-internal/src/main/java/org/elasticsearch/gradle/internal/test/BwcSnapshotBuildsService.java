/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test;

import org.gradle.api.invocation.Gradle;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Inject;

/**
 * Shares the DRA build each {@code :distribution:bwc:*} project resolved for its version with the test tasks of other
 * projects, so that a test JVM knows exactly which snapshot it runs against (see {@link BwcSnapshotPins}).
 *
 * <p>Builds are registered during configuration. Lookups return providers that read the registry only when queried,
 * so the order in which projects are configured does not matter.
 */
public abstract class BwcSnapshotBuildsService implements BuildService<BuildServiceParameters.None> {

    private static final String NAME = "bwcSnapshotBuilds";

    private final ProviderFactory providerFactory;
    private final Map<String, Provider<String>> buildIdsByVersion = new ConcurrentHashMap<>();

    @Inject
    public BwcSnapshotBuildsService(ProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
    }

    public static Provider<BwcSnapshotBuildsService> register(Gradle gradle) {
        return gradle.getSharedServices().registerIfAbsent(NAME, BwcSnapshotBuildsService.class, spec -> {});
    }

    /**
     * @param buildId the DRA build ID resolved for {@code version}, e.g. {@code 9.4.10-3710ab3e}, or {@code ""} when the
     *                version is built from source
     */
    public void register(String version, Provider<String> buildId) {
        buildIdsByVersion.put(version, buildId);
    }

    /**
     * The DRA build resolved for {@code version}: empty when no bwc project resolves that version (a released one, for
     * instance), {@code ""} when it is built from source.
     */
    public Provider<String> buildId(String version) {
        return providerFactory.provider(() -> buildIdsByVersion.get(version)).flatMap(buildId -> buildId);
    }
}
