/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.agent.collector.AbstractCollector;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.elasticsearch.marvel.MonitoringLicensee;
import org.elasticsearch.shield.InternalClient;
import org.elasticsearch.shield.Security;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Collector for the Recovery API.
 * <p>
 * This collector runs on the master node only and collects a {@link IndexRecoveryMonitoringDoc} document
 * for every index that has on-going shard recoveries.
 */
public class IndexRecoveryCollector extends AbstractCollector<IndexRecoveryCollector> {

    public static final String NAME = "index-recovery-collector";

    private final Client client;

    @Inject
    public IndexRecoveryCollector(Settings settings, ClusterService clusterService,
                                  MonitoringSettings monitoringSettings, MonitoringLicensee licensee, InternalClient client) {
        super(settings, NAME, clusterService, monitoringSettings, licensee);
        this.client = client;
    }

    @Override
    protected boolean shouldCollect() {
        return super.shouldCollect() && isLocalNodeMaster();
    }

    @Override
    protected Collection<MonitoringDoc> doCollect() throws Exception {
        List<MonitoringDoc> results = new ArrayList<>(1);
        try {
            RecoveryResponse recoveryResponse = client.admin().indices().prepareRecoveries()
                    .setIndices(monitoringSettings.indices())
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                    .setActiveOnly(monitoringSettings.recoveryActiveOnly())
                    .get(monitoringSettings.recoveryTimeout());

            if (recoveryResponse.hasRecoveries()) {
                IndexRecoveryMonitoringDoc indexRecoveryDoc = new IndexRecoveryMonitoringDoc(monitoringId(), monitoringVersion());
                indexRecoveryDoc.setClusterUUID(clusterUUID());
                indexRecoveryDoc.setTimestamp(System.currentTimeMillis());
                indexRecoveryDoc.setSourceNode(localNode());
                indexRecoveryDoc.setRecoveryResponse(recoveryResponse);
                results.add(indexRecoveryDoc);
            }
        } catch (IndexNotFoundException e) {
            if (Security.enabled(settings) && IndexNameExpressionResolver.isAllIndices(Arrays.asList(monitoringSettings.indices()))) {
                logger.debug("collector [{}] - unable to collect data for missing index [{}]", name(), e.getIndex());
            } else {
                throw e;
            }
        }
        return Collections.unmodifiableCollection(results);
    }
}
