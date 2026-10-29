/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.rest;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;

import java.util.function.Supplier;

/**
 * Builds the {@link ElasticsearchCluster} shared by the searchable-snapshots REST test suites. The cluster runs the
 * default distribution because the tests exercise the security and searchable-snapshots x-pack features, and it exposes
 * a caller-provided {@code path.repo} so each suite can register a filesystem repository against its own temp directory.
 */
final class SearchableSnapshotsRestTestCluster {

    private SearchableSnapshotsRestTestCluster() {}

    static ElasticsearchCluster buildCluster(Supplier<String> repoPath) {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .setting("path.repo", repoPath)
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
            .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
            .setting("xpack.searchable_snapshots.cache_fetch_async_thread_pool.keep_alive", "0ms")
            .setting("xpack.security.enabled", "true")
            .user("admin", "admin-password", "superuser", false)
            .build();
    }
}
