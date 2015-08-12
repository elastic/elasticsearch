/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector;


import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettingsService;

import java.util.Collection;

public abstract class AbstractCollector<T> extends AbstractLifecycleComponent<T> implements Collector<T> {

    private final String name;

    protected final ClusterService clusterService;
    protected final ClusterName clusterName;
    protected final MarvelSettingsService marvelSettings;

    @Inject
    public AbstractCollector(Settings settings, String name, ClusterService clusterService,
                             ClusterName clusterName, MarvelSettingsService marvelSettings) {
        super(settings);
        this.name = name;
        this.clusterService = clusterService;
        this.clusterName = clusterName;
        this.marvelSettings = marvelSettings;
    }

    @Override
    public String name() {
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
     * Indicates if the current collector should
     * be executed on master node only.
     */
    protected boolean masterOnly() {
        return false;
    }

    @Override
    public Collection<MarvelDoc> collect() {
        if (masterOnly() && !clusterService.state().nodes().localNodeMaster()) {
            logger.trace("collector [{}] runs on master only", name());
            return null;
        }

        try {
            return doCollect();
        } catch (ElasticsearchTimeoutException e) {
            logger.error("collector [{}] timed out when collecting data");
        } catch (Exception e) {
            logger.error("collector [{}] throws exception when collecting data", e, name());
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
}