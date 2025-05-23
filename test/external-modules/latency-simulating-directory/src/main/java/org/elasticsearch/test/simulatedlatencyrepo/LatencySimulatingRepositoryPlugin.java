/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.simulatedlatencyrepo;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Map;

/**
 * Repository wrapping an FsRepository and adding a configurable latency to each
 * blob store read.
 */
public class LatencySimulatingRepositoryPlugin extends Plugin implements RepositoryPlugin {

    public static final String TYPE = "latency-simulating";

    @Override
    public Map<String, Repository.Factory> getRepositories(
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        BigArrays bigArrays,
        RecoverySettings recoverySettings,
        RepositoriesMetrics repositoriesMetrics
    ) {
        return Map.of(
            TYPE,
            (projectId, metadata) -> new LatencySimulatingBlobStoreRepository(
                metadata,
                env,
                namedXContentRegistry,
                clusterService,
                bigArrays,
                recoverySettings,
                buildSimulator(metadata.settings())
            )
        );
    }

    private static Runnable buildSimulator(Settings settings) {
        long sleepyTime = settings.getAsLong("latency", 0L);
        if (sleepyTime == 0L) {
            return () -> {};
        }
        return () -> {
            try {
                Thread.sleep(sleepyTime);
            } catch (InterruptedException e) {
                throw new AssertionError("BlobRepository read interrupted!");
            }
        };
    }
}
