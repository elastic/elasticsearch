/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.graph;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.XPackFeatureSet;

/**
 *
 */
public class GraphFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final GraphLicensee licensee;

    @Inject
    public GraphFeatureSet(Settings settings, @Nullable  GraphLicensee licensee) {
        this.enabled = Graph.enabled(settings);
        this.licensee = licensee;
    }

    @Override
    public String name() {
        return Graph.NAME;
    }

    @Override
    public String description() {
        return "Graph Data Exploration for the Elastic Stack";
    }

    @Override
    public boolean available() {
        return licensee != null && licensee.isAvailable();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }
}
