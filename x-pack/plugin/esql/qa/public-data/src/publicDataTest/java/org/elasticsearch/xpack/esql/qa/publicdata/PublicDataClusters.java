/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.xpack.esql.datasources.Federation;

/**
 * The shared 3-node DEFAULT-distribution cluster the suite runs against: two data nodes plus a
 * coordinating-only node, mirroring a realistic small deployment for remote scans.
 *
 * <p>Deliberate absences, all load-bearing:
 * <ul>
 *   <li><b>No {@code ESQL_EXTERNAL_DATASOURCES_LOCAL} feature flag</b> — the suite must be
 *       structurally incapable of reading {@code file://}, so a locally downloaded copy can never
 *       masquerade as the pinned remote object. HTTP/GCS/AZURE stay pre-enabled but inert, so
 *       activating a backup/future provider needs no cluster change.</li>
 *   <li><b>No {@code path.repo}, no {@code local_allowed_paths}</b> — nothing on disk to read.</li>
 *   <li><b>No keystore, no credentials, no project-encryption-key</b> — anonymous public access
 *       carries no secrets, so registering data sources encrypts nothing.</li>
 * </ul>
 */
public final class PublicDataClusters {

    private PublicDataClusters() {}

    public static ElasticsearchCluster shared() {
        String heap = PublicDataFilters.fromSystemProperties().heap();
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .shared(true)
            .nodes(3)
            // coordinating-only third node: remote reads fan out from a node that holds no data
            .node(2, node -> node.setting("node.roles", "[]"))
            .feature(FeatureFlag.ESQL_EXTERNAL_DATASOURCES_HTTP)
            .feature(FeatureFlag.ESQL_EXTERNAL_GCS)
            .feature(FeatureFlag.ESQL_EXTERNAL_AZURE)
            .setting("xpack.security.enabled", "false")
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
            // Raised external-read knobs: public corpora legitimately have many objects per prefix,
            // and public buckets throttle, so give discovery and retries generous headroom.
            // Diagnostic note from the first real runs: a 429 on /_query is NOT necessarily store
            // throttling — ES|QL surfaces request-circuit-breaker trips as 429 too, and only the
            // response's cause chain (CircuitBreakingException vs throttle retries exhausted)
            // tells them apart. Oversized aggregation states are fixed with in-query filters in
            // the workload, never by raising heap first.
            .setting("esql.external.max_discovered_files", "100000")
            .setting("esql.external.max_glob_expansion", "1000")
            .setting("esql.external.max_concurrent_requests", "32")
            .setting("esql.external.throttle_max_retry_duration", "300")
            .jvmArg("-Xms" + heap)
            .jvmArg("-Xmx" + heap)
            // Arrow's unsafe allocator needs java.nio internals opened, and must be selected before
            // any Arrow class loads.
            .jvmArg("--add-opens=java.base/java.nio=ALL-UNNAMED")
            .jvmArg("-Darrow.allocation.manager.type=Unsafe")
            .build();
    }
}
