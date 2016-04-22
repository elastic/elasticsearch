/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.elasticsearch.marvel.MonitoringLicensee;

import java.util.Collection;

public abstract class AbstractCollector<T> extends AbstractLifecycleComponent<T> implements Collector<T> {

    private final String name;

    protected final ClusterService clusterService;
    protected final MonitoringSettings monitoringSettings;
    protected final MonitoringLicensee licensee;

    @Inject
    public AbstractCollector(Settings settings, String name, ClusterService clusterService,
                             MonitoringSettings monitoringSettings, MonitoringLicensee licensee) {
        super(settings);
        this.name = name;
        this.clusterService = clusterService;
        this.monitoringSettings = monitoringSettings;
        this.licensee = licensee;
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
    public T start() {
        logger.debug("starting collector [{}]", name());
        return super.start();
    }

    @Override
    protected void doStart() {
    }

    /**
     * Indicates if the current collector is allowed to collect data
     */
    protected boolean shouldCollect() {
        if (!licensee.collectionEnabled()) {
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
    public T stop() {
        logger.debug("stopping collector [{}]", name());
        return super.stop();
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
