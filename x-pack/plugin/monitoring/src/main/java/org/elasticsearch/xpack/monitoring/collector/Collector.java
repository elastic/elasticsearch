/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.settings.Setting.Property;
import static org.elasticsearch.common.settings.Setting.stringListSetting;
import static org.elasticsearch.common.settings.Setting.timeSetting;

/**
 * {@link Collector} are used to collect monitoring data about the cluster, nodes and indices.
 */
public abstract class Collector {

    /**
     * List of indices names whose stats will be exported (default to all indices)
     */
    public static final Setting<List<String>> INDICES = stringListSetting(
        collectionSetting("indices"),
        Property.Dynamic,
        Property.NodeScope,
        Setting.Property.DeprecatedWarning
    );

    private final String name;
    private final Setting<TimeValue> collectionTimeoutSetting;

    protected final ClusterService clusterService;
    protected final XPackLicenseState licenseState;
    protected final Logger logger;

    public Collector(
        final String name,
        final ClusterService clusterService,
        final Setting<TimeValue> timeoutSetting,
        final XPackLicenseState licenseState
    ) {
        this.name = name;
        this.clusterService = clusterService;
        this.collectionTimeoutSetting = timeoutSetting;
        this.licenseState = licenseState;
        this.logger = LogManager.getLogger(getClass());
    }

    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return name();
    }

    /**
     * Indicates if the current collector is allowed to collect data
     *
     * @param isElectedMaster true if the current local node is the elected master node
     */
    protected boolean shouldCollect(final boolean isElectedMaster) {
        return true;
    }

    public Collection<MonitoringDoc> collect(final long timestamp, final long interval, final ClusterState clusterState) {
        try {
            final boolean isElectedMaster = clusterState.getNodes().isLocalNodeElectedMaster();
            if (shouldCollect(isElectedMaster)) {
                logger.trace("collector [{}] - collecting data...", name());
                return doCollect(convertNode(timestamp, clusterService.localNode()), interval, clusterState);
            }
        } catch (ElasticsearchTimeoutException e) {
            logger.error("collector [{}] timed out when collecting data: {}", name(), e.getMessage());
        } catch (Exception e) {
            logger.error((Supplier<?>) () -> "collector [" + name() + "] failed to collect data", e);
        }
        return null;
    }

    protected abstract Collection<MonitoringDoc> doCollect(MonitoringDoc.Node node, long interval, ClusterState clusterState)
        throws Exception;

    /**
     * Returns a timestamp to use in {@link MonitoringDoc}
     *
     * @return the timestamp
     */
    protected static long timestamp() {
        return System.currentTimeMillis();
    }

    /**
     * Extracts the current cluster's UUID from a {@link ClusterState}
     *
     * @param clusterState the {@link ClusterState}
     * @return the cluster's UUID
     */
    protected static String clusterUuid(final ClusterState clusterState) {
        return clusterState.metadata().clusterUUID();
    }

    /**
     * Returns the value of the collection timeout configured for the current {@link Collector}.
     *
     * @return the collection timeout, or {@code null} if the collector has not timeout defined.
     */
    public TimeValue getCollectionTimeout() {
        if (collectionTimeoutSetting == null) {
            return null;
        }
        return clusterService.getClusterSettings().get(collectionTimeoutSetting);
    }

    /**
     * Returns the names of indices Monitoring collects data from.
     *
     * @return a array of indices
     */
    public String[] getCollectionIndices() {
        final List<String> indices = clusterService.getClusterSettings().get(INDICES);
        assert indices != null;
        if (indices.isEmpty()) {
            return Strings.EMPTY_ARRAY;
        } else {
            return indices.toArray(new String[indices.size()]);
        }
    }

    /**
     * Creates a {@link MonitoringDoc.Node} from a {@link DiscoveryNode} and a timestamp, copying over the
     * required information.
     *
     * @param timestamp the node's timestamp
     * @param node the {@link DiscoveryNode}
     *
     * @return a {@link MonitoringDoc.Node} instance, or {@code null} if the given discovery node is null.
     */
    public static MonitoringDoc.Node convertNode(final long timestamp, final @Nullable DiscoveryNode node) {
        if (node == null) {
            return null;
        }
        return new MonitoringDoc.Node(
            node.getId(),
            node.getHostName(),
            node.getAddress().toString(),
            node.getHostAddress(),
            node.getName(),
            timestamp
        );
    }

    protected static String collectionSetting(final String settingName) {
        Objects.requireNonNull(settingName, "setting name must not be null");
        return XPackField.featureSettingPrefix(XPackField.MONITORING) + ".collection." + settingName;
    }

    protected static Setting<TimeValue> collectionTimeoutSetting(final String settingName) {
        String name = collectionSetting(settingName);
        return timeSetting(name, TimeValue.timeValueSeconds(10), Property.Dynamic, Property.NodeScope, Property.DeprecatedWarning);
    }
}
