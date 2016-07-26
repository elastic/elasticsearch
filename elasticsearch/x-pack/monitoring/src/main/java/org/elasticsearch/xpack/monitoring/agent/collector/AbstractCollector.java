/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.agent.collector;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.core.XPackLicenseState;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.agent.exporter.MonitoringDoc;

import java.util.Collection;

public abstract class AbstractCollector extends AbstractLifecycleComponent implements Collector {

    private final String name;

    protected final ClusterService clusterService;
    protected final MonitoringSettings monitoringSettings;
    protected final XPackLicenseState licenseState;

    @Inject
    public AbstractCollector(Settings settings, String name, ClusterService clusterService,
                             MonitoringSettings monitoringSettings, XPackLicenseState licenseState) {
        super(settings);
        this.name = name;
        this.clusterService = clusterService;
        this.monitoringSettings = monitoringSettings;
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public void start() {
        logger.debug("starting collector [{}]", name());
        super.start();
    }

    @Override
    protected void doStart() {
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

    @Override
    public Collection<MonitoringDoc> collect() {
        try {
            if (shouldCollect()) {
                logger.trace("collector [{}] - collecting data...", name());
                return doCollect();
            }
        } catch (ElasticsearchTimeoutException e) {
            logger.error("collector [{}] timed out when collecting data", name());
        } catch (Exception e) {
            logger.error("collector [{}] - failed collecting data", e, name());
        }
        return null;
    }

    protected abstract Collection<MonitoringDoc> doCollect() throws Exception;

    @Override
    public void stop() {
        logger.debug("stopping collector [{}]", name());
        super.stop();
    }

    @Override
    protected void doStop() {
    }

    @Override
    public void close() {
        logger.trace("closing collector [{}]", name());
        super.close();
    }

    @Override
    protected void doClose() {
    }

    protected String clusterUUID() {
        return clusterService.state().metaData().clusterUUID();
    }


    protected DiscoveryNode localNode() {
        return clusterService.localNode();
    }

    protected String monitoringId() {
        // Collectors always collects data for Elasticsearch
        return MonitoredSystem.ES.getSystem();
    }

    protected String monitoringVersion() {
        // Collectors always collects data for the current version of Elasticsearch
        return Version.CURRENT.toString();
    }
}
