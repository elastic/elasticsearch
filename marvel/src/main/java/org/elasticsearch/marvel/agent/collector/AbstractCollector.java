/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector;


import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.license.MarvelLicensee;

import java.util.Collection;

public abstract class AbstractCollector<T> extends AbstractLifecycleComponent<T> implements Collector<T> {

    private final String name;

    protected final ClusterService clusterService;
    protected final MarvelSettings marvelSettings;
    protected final MarvelLicensee licensee;

    @Inject
    public AbstractCollector(Settings settings, String name, ClusterService clusterService, MarvelSettings marvelSettings, MarvelLicensee licensee) {
        super(settings);
        this.name = name;
        this.clusterService = clusterService;
        this.marvelSettings = marvelSettings;
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
        return clusterService.state().nodes().localNodeMaster();
    }

    @Override
    public Collection<MarvelDoc> collect() {
        try {
            if (shouldCollect()) {
                logger.trace("collector [{}] - collecting data...", name());
                return doCollect();
            }
        } catch (ElasticsearchTimeoutException e) {
            logger.error("collector [{}] timed out when collecting data");
        } catch (Exception e) {
            logger.error("collector [{}] - failed collecting data", e, name());
        }
        return null;
    }

    protected abstract Collection<MarvelDoc> doCollect() throws Exception;

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
}