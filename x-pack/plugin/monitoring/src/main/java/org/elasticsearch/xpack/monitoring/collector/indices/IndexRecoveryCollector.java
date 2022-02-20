/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector.indices;

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.xpack.monitoring.collector.TimeoutUtils.ensureNoTimeouts;

/**
 * Collector for the Recovery API.
 * <p>
 * This collector runs on the master node only and collects a {@link IndexRecoveryMonitoringDoc}
 * document for every index that has on-going shard recoveries.
 */
public class IndexRecoveryCollector extends Collector {

    /**
     * Timeout value when collecting the recovery information (default to 10s)
     */
    public static final Setting<TimeValue> INDEX_RECOVERY_TIMEOUT = collectionTimeoutSetting("index.recovery.timeout");

    /**
     * Flag to indicate if only active recoveries should be collected (default to false: all recoveries are collected)
     */
    public static final Setting<Boolean> INDEX_RECOVERY_ACTIVE_ONLY = boolSetting(
        collectionSetting("index.recovery.active_only"),
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope,
        Setting.Property.DeprecatedWarning
    );

    private final Client client;

    public IndexRecoveryCollector(final ClusterService clusterService, final XPackLicenseState licenseState, final Client client) {
        super(IndexRecoveryMonitoringDoc.TYPE, clusterService, INDEX_RECOVERY_TIMEOUT, licenseState);
        this.client = Objects.requireNonNull(client);
    }

    boolean getActiveRecoveriesOnly() {
        return clusterService.getClusterSettings().get(INDEX_RECOVERY_ACTIVE_ONLY);
    }

    @Override
    protected boolean shouldCollect(final boolean isElectedMaster) {
        return isElectedMaster && super.shouldCollect(isElectedMaster);
    }

    @Override
    protected Collection<MonitoringDoc> doCollect(final MonitoringDoc.Node node, final long interval, final ClusterState clusterState) {
        List<MonitoringDoc> results = new ArrayList<>(1);
        RecoveryResponse recoveryResponse = client.admin()
            .indices()
            .prepareRecoveries()
            .setIndices(getCollectionIndices())
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setActiveOnly(getActiveRecoveriesOnly())
            .setTimeout(getCollectionTimeout())
            .get();

        ensureNoTimeouts(getCollectionTimeout(), recoveryResponse);

        if (recoveryResponse.hasRecoveries()) {
            final String clusterUuid = clusterUuid(clusterState);
            results.add(new IndexRecoveryMonitoringDoc(clusterUuid, timestamp(), interval, node, recoveryResponse));
        }
        return Collections.unmodifiableCollection(results);
    }
}
