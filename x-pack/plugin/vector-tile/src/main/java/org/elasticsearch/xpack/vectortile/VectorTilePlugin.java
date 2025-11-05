/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.vectortile;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.vectortile.rest.RestVectorTileAction;

import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Plugin for vector tile search functionality in Elasticsearch.
 * <p>
 * This plugin provides the ability to return search results as Mapbox Vector Tiles (MVT),
 * which is a compact binary format for efficiently transmitting geographic data for rendering
 * in maps. The plugin aggregates geo_point and geo_shape data into vector tiles at specified
 * zoom levels.
 * </p>
 * <p><b>Usage Example:</b></p>
 * <pre>{@code
 * GET /my-index/_mvt/geo_field/15/5242/12661
 * {
 *   "grid_precision": 2,
 *   "fields": ["field1", "field2"],
 *   "query": {
 *     "match_all": {}
 *   }
 * }
 * }</pre>
 */
public class VectorTilePlugin extends Plugin implements ActionPlugin {

    /**
     * Returns the X-Pack license state.
     * <p>
     * This method can be overridden by tests to provide a different license state.
     * </p>
     *
     * @return the shared X-Pack license state
     */
    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    /**
     * Returns the REST handlers provided by this plugin.
     * <p>
     * Registers the REST endpoint for vector tile search at
     * {@code /<index>/_mvt/<field>/<zoom>/<x>/<y>}.
     * </p>
     *
     * @param settings the node settings
     * @param namedWriteableRegistry the named writeable registry
     * @param restController the REST controller
     * @param clusterSettings the cluster settings
     * @param indexScopedSettings the index-scoped settings
     * @param settingsFilter the settings filter
     * @param indexNameExpressionResolver the index name expression resolver
     * @param nodesInCluster supplier for discovery nodes
     * @param clusterSupportsFeature predicate to check feature support
     * @return a list containing the vector tile REST handler
     */
    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return List.of(new RestVectorTileAction(restController.getSearchUsageHolder(), settings));
    }
}
