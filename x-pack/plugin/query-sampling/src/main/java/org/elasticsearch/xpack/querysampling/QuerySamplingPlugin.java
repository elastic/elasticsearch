/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.querysampling.capture.QueryCaptureFilter;

import java.util.Collection;
import java.util.List;

/**
 * Keeps a small, continuously maintained sample of live kNN queries so that production recall can be
 * estimated without replaying all traffic. The pipeline runs on the coordinating node and is designed
 * to stay off the search critical path.
 */
public class QuerySamplingPlugin extends Plugin implements ActionPlugin {

    private static final Logger logger = LogManager.getLogger(QuerySamplingPlugin.class);

    private final SetOnce<QueryCaptureFilter> captureFilter = new SetOnce<>();

    @Override
    public List<Setting<?>> getSettings() {
        return QuerySamplingSettings.getSettings();
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        captureFilter.set(
            new QueryCaptureFilter(
                services.clusterService().getClusterSettings(),
                captured -> logger.trace("captured kNN search on field [{}] with {} hits", captured.query().field(), captured.hits().size())
            )
        );
        return List.of();
    }

    @Override
    public Collection<MappedActionFilter> getMappedActionFilters() {
        return List.of(captureFilter.get());
    }
}
