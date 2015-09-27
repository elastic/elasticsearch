/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.marvel.agent.collector.AbstractCollector;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.license.LicenseService;
import org.elasticsearch.marvel.shield.SecuredClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Collector for registered licenses and additional cluster information.
 * <p>
 * This collector runs on the master node and collect data about all
 * known licenses that are currently registered. It also ships some stats
 * about the cluster (to be used in Phone Home feature).
 */
public class ClusterInfoCollector extends AbstractCollector<ClusterInfoMarvelDoc> {

    public static final String NAME = "cluster-info-collector";
    public static final String TYPE = "cluster_info";

    private final ClusterName clusterName;
    private final LicenseService licenseService;
    private final Client client;

    @Inject
    public ClusterInfoCollector(Settings settings, ClusterService clusterService, MarvelSettings marvelSettings, LicenseService licenseService,
                                ClusterName clusterName, SecuredClient client) {
        super(settings, NAME, clusterService, marvelSettings, licenseService);
        this.clusterName = clusterName;
        this.licenseService = licenseService;
        this.client = client;
    }

    @Override
    protected boolean shouldCollect() {
        // This collector can always collect data on the master node
        return isLocalNodeMaster();
    }

    @Override
    protected Collection<MarvelDoc> doCollect() throws Exception {
        List<MarvelDoc> results = new ArrayList<>(1);

        // Retrieves all licenses
        List<License> licenses = licenseService.licenses();

        // Retrieves additional cluster stats
        ClusterStatsResponse clusterStats = client.admin().cluster().prepareClusterStats().get(marvelSettings.clusterStatsTimeout());

        String clusterUUID = clusterUUID();
        results.add(new ClusterInfoMarvelDoc(MarvelSettings.MARVEL_DATA_INDEX_NAME, TYPE, clusterUUID, clusterUUID, System.currentTimeMillis(),
                clusterName.value(), Version.CURRENT.toString(), licenses, clusterStats));
        return Collections.unmodifiableCollection(results);
    }
}
