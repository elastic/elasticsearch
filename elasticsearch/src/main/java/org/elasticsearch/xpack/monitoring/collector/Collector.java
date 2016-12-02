/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

import java.util.Collection;

/**
 * {@link Collector} are used to collect monitoring data about the cluster, nodes and indices.
 */
public abstract class Collector extends AbstractComponent {

    private final String name;

    protected final ClusterService clusterService;
    protected final MonitoringSettings monitoringSettings;
    protected final XPackLicenseState licenseState;

    public Collector(Settings settings, String name, ClusterService clusterService,
                     MonitoringSettings monitoringSettings, XPackLicenseState licenseState) {
        super(settings);
        this.name = name;
        this.clusterService = clusterService;
        this.monitoringSettings = monitoringSettings;
        this.licenseState = licenseState;
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
     */
    protected boolean shouldCollect() {
        if (licenseState.isMonitoringAllowed() == false) {
            logger.trace("collector [{}] can not collect data due to invalid license", name());
            return false;
        }
        return true;
    }

    protected boolean isLocalNodeMaster() {
        return clusterService.state().nodes().isLocalNodeElectedMaster();
    }

    public Collection<MonitoringDoc> collect() {
        try {
            if (shouldCollect()) {
                logger.trace("collector [{}] - collecting data...", name());
                return doCollect();
            }
        } catch (ElasticsearchTimeoutException e) {
            logger.error((Supplier<?>) () -> new ParameterizedMessage("collector [{}] timed out when collecting data", name()));
        } catch (Exception e) {
            logger.error((Supplier<?>) () -> new ParameterizedMessage("collector [{}] failed to collect data", name()), e);
        }
        return null;
    }

    protected abstract Collection<MonitoringDoc> doCollect() throws Exception;

    protected String clusterUUID() {
        return clusterService.state().metaData().clusterUUID();
    }

    protected DiscoveryNode localNode() {
        return clusterService.localNode();
    }

    protected static String monitoringId() {
        // Collectors always collects data for Elasticsearch
        return MonitoredSystem.ES.getSystem();
    }

    protected static String monitoringVersion() {
        // Collectors always collects data for the current version of Elasticsearch
        return Version.CURRENT.toString();
    }
}
