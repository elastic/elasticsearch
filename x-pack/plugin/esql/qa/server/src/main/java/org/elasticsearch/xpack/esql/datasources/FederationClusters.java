/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;

/**
 * Turns ES|QL federation on for a test cluster, on the platforms that can run it.
 *
 * <p>Two things have to line up and they are easy to get half-right, which is why this lives in one place rather
 * than being repeated per cluster builder:
 * <ul>
 *   <li>{@code esql.federation.enabled} is only a <em>known</em> setting while the feature is registered. Writing it
 *       where the feature cannot exist fails node startup with the framework's {@code unknown setting} error, which
 *       takes down every suite on that cluster rather than just the external-source ones.</li>
 *   <li>Registration itself defaults off on Windows, so setting {@code esql.federation.enabled} there is not enough
 *       on its own — the node also has to be asked to register the feature.</li>
 * </ul>
 *
 * <p>Suites that call this must still skip themselves when it returns {@code false}; the usual check is
 * {@code assumeTrue(Federation.SUPPORTED)}, which holds everywhere except a Windows release build.
 */
public final class FederationClusters {

    private FederationClusters() {}

    /**
     * Enables federation on {@code spec} where the platform supports it, and does nothing where it does not.
     *
     * @return whether the cluster was configured for federation, i.e. {@link Federation#SUPPORTED}
     */
    public static boolean enable(LocalClusterSpecBuilder<?> spec) {
        if (Federation.SUPPORTED == false) {
            return false;
        }
        spec.setting(Federation.FEDERATION_ENABLED.getKey(), "true");
        // Only where the default is off, so this does not fight the suites that deliberately unregister the feature.
        if (Federation.DEFAULT_REGISTERED == false) {
            spec.systemProperty(Federation.REGISTER_PROPERTY, "true");
        }
        return true;
    }
}
