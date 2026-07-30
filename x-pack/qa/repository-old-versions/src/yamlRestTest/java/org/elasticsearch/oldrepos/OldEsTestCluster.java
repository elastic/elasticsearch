/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos;

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
     * Creates a new {@link OldElasticsearchContainer} for the version and repo location supplied
     * via {@code tests.es.version} / {@code tests.repo.location} system properties.
     */
    static OldElasticsearchContainer newContainer() {
        return new OldElasticsearchContainer(System.getProperty("tests.es.version"), System.getProperty("tests.repo.location"));
    }

    /**
     * Creates the shared two-node cluster configuration used by all old-repo snapshot tests.
     */
    static ElasticsearchCluster newCluster() {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .nodes(2)
            .setting("path.repo", () -> System.getProperty("tests.repo.location"))
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.security.enabled", "true")
            .user("admin", "admin-password", "superuser", false)
            .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
            .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
            .build();
    }
}
