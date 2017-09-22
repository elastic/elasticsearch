/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.indices;

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.collector.Collector;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Collector for the Recovery API.
 * <p>
 * This collector runs on the master node only and collects a {@link IndexRecoveryMonitoringDoc}
 * document for every index that has on-going shard recoveries.
 */
public class IndexRecoveryCollector extends Collector {

    private final Client client;

    public IndexRecoveryCollector(final Settings settings,
                                  final ClusterService clusterService,
                                  final MonitoringSettings monitoringSettings,
                                  final XPackLicenseState licenseState,
                                  final Client client) {

        super(settings, IndexRecoveryMonitoringDoc.TYPE, clusterService, monitoringSettings, licenseState);
        this.client = Objects.requireNonNull(client);
    }

    @Override
    protected boolean shouldCollect() {
        return super.shouldCollect() && isLocalNodeMaster();
    }

    @Override
    protected Collection<MonitoringDoc> doCollect(final MonitoringDoc.Node node) throws Exception {
        List<MonitoringDoc> results = new ArrayList<>(1);
        RecoveryResponse recoveryResponse = client.admin().indices().prepareRecoveries()
                .setIndices(monitoringSettings.indices())
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setActiveOnly(monitoringSettings.recoveryActiveOnly())
                .get(monitoringSettings.recoveryTimeout());

        if (recoveryResponse.hasRecoveries()) {
            results.add(new IndexRecoveryMonitoringDoc(clusterUUID(), timestamp(), node, recoveryResponse));
        }
        return Collections.unmodifiableCollection(results);
    }
}
