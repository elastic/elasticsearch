/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.fixtures.oldelasticsearch.OldElasticsearchContainer;

/**
 * Factory helpers shared by old-ES snapshot tests to avoid copy-pasting identical cluster
 * configuration across {@link OldRepositoryAccessIT}, {@link OldMappingsIT}, and
 * {@link DocValueOnlyFieldsIT}.
 */
final class OldEsTestCluster {

    private OldEsTestCluster() {}

    /**
     * Computes a per-test-class snapshot repository directory nested under the shared
     * {@code tests.repo.location} base directory.
     * <p>
     * Each test class must get its own isolated subdirectory because
     * {@link OldElasticsearchContainer}'s entrypoint wipes its bind-mounted repo directory on
     * every container start. All three old-repo test classes share the same Gradle task and run
     * in separate, potentially concurrent, forked JVMs; if they shared one directory, one
     * class's container startup could wipe out snapshot data another class was still using,
     * causing spurious recovery failures after a cluster restart.
     */
    static String repoLocation(Class<?> testClass) {
        return PathUtils.get(System.getProperty("tests.repo.location")).resolve(testClass.getName()).toString();
    }

    /**
     * Creates a new {@link OldElasticsearchContainer} for the version supplied via the
     * {@code tests.es.version} system property, bind-mounting the per-{@code testClass} repo
     * directory computed by {@link #repoLocation(Class)}.
     */
    static OldElasticsearchContainer newContainer(Class<?> testClass) {
        return new OldElasticsearchContainer(System.getProperty("tests.es.version"), repoLocation(testClass));
    }

    /**
     * Creates the shared two-node cluster configuration used by all old-repo snapshot tests,
     * with {@code path.repo} set to the per-{@code testClass} repo directory computed by
     * {@link #repoLocation(Class)}.
     */
    static ElasticsearchCluster newCluster(Class<?> testClass) {
        String repoLocation = repoLocation(testClass);
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .nodes(2)
            .setting("path.repo", () -> repoLocation)
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.security.enabled", "true")
            .user("admin", "admin-password", "superuser", false)
            .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
            .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
            .build();
    }
}
