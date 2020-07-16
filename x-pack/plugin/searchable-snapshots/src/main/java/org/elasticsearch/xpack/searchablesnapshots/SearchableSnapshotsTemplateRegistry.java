/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.SEARCHABLE_SNAPSHOTS_ORIGIN;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.SEARCHABLE_SNAPSHOTS_FEATURE_ENABLED;

public class SearchableSnapshotsTemplateRegistry extends IndexTemplateRegistry {

    // history (please add a comment why you increased the version here)
    // version 1: initial
    static final int INDEX_TEMPLATE_VERSION = 1;

    static final String SNAPSHOTS_CACHE_TEMPLATE_NAME = ".snapshots";

    private SearchableSnapshotsTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
    }

    public static SearchableSnapshotsTemplateRegistry register(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        // registers a cluster state listener
        return new SearchableSnapshotsTemplateRegistry(nodeSettings, clusterService, threadPool, client, xContentRegistry);
    }

    @Override
    protected String getOrigin() {
        return SEARCHABLE_SNAPSHOTS_ORIGIN;
    }

    @Override
    protected boolean requiresMasterNode() {
        return true;
    }

    @Override
    protected List<IndexTemplateConfig> getLegacyTemplateConfigs() {
        if (SEARCHABLE_SNAPSHOTS_FEATURE_ENABLED) {
            return List.of(
                new IndexTemplateConfig(
                    SNAPSHOTS_CACHE_TEMPLATE_NAME,
                    "/searchable-snapshots-template.json",
                    INDEX_TEMPLATE_VERSION,
                    "xpack.searchable_snapshots.template.version",
                    Map.of("xpack.searchable_snapshots.version", Version.CURRENT.toString())
                )
            );
        }
        return List.of();
    }
}
