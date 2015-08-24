/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.licenses;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.marvel.agent.collector.AbstractCollector;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.license.LicenseService;

import java.util.Collection;
import java.util.List;

/**
 * Collector for registered licenses.
 * <p/>
 * This collector runs on the master node and collect data about all
 * known licenses that are currently registered. Each license is
 * collected as a {@link LicensesMarvelDoc} document.
 */
public class LicensesCollector extends AbstractCollector<LicensesMarvelDoc> {

    public static final String NAME = "licenses-collector";
    public static final String TYPE = "cluster_licenses";

    private final ClusterName clusterName;
    private final LicenseService licenseService;

    @Inject
    public LicensesCollector(Settings settings, ClusterService clusterService, MarvelSettings marvelSettings, LicenseService licenseService,
                             ClusterName clusterName) {
        super(settings, NAME, clusterService, marvelSettings, licenseService);
        this.clusterName = clusterName;
        this.licenseService = licenseService;
    }

    @Override
    protected boolean canCollect() {
        // This collector can always collect data on the master node
        return isLocalNodeMaster();
    }

    @Override
    protected Collection<MarvelDoc> doCollect() throws Exception {
        ImmutableList.Builder<MarvelDoc> results = ImmutableList.builder();

        List<License> licenses = licenseService.licenses();
        if (licenses != null) {
            String clusterUUID = clusterUUID();
            results.add(new LicensesMarvelDoc(MarvelSettings.MARVEL_DATA_INDEX_NAME, TYPE, clusterUUID, clusterUUID, System.currentTimeMillis(),
                    clusterName.value(), Version.CURRENT.toString(), licenses));
        }
        return results.build();
    }
}
