/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.vectortile;

import org.elasticsearch.Build;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.geo.GeoFormatterFactory;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.vectortile.feature.FeatureFactory;
import org.elasticsearch.xpack.vectortile.rest.RestVectorTileAction;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

public class VectorTilePlugin extends Plugin implements ActionPlugin, MapperPlugin {

    // to be overriden by tests
    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    private static final Boolean VECTOR_TILE_FEATURE_FLAG_REGISTERED;

    static {
        final String property = System.getProperty("es.vector_tile_feature_flag_registered");
        if (Build.CURRENT.isSnapshot() && property != null) {
            throw new IllegalArgumentException("es.vector_tile_feature_flag_registered is only supported in non-snapshot builds");
        }
        VECTOR_TILE_FEATURE_FLAG_REGISTERED = Booleans.parseBoolean(property, null);
    }

    public boolean isVectorTileEnabled() {
        return Build.CURRENT.isSnapshot() || (VECTOR_TILE_FEATURE_FLAG_REGISTERED != null && VECTOR_TILE_FEATURE_FLAG_REGISTERED);
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        if (isVectorTileEnabled()) {
            return List.of(new RestVectorTileAction());
        } else {
            return List.of();
        }
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        FixedExecutorBuilder indexing = new FixedExecutorBuilder(
            settings,
            "vector_tile_generation",
            1,
            -1,
            "thread_pool.vectortile",
            false
        );
        return Collections.singletonList(indexing);
    }

    @Override
    public Map<String, GeoFormatterFactory.GeoFormatterEngine> getGeoFormatters() {
        return Map.of("mvt", param -> {
            final String[] parts = param.split("@", 3);
            if (parts.length > 2) {
                throw new IllegalArgumentException(
                    "Invalid mvt formatter parameter [" + param + "]. Must have the form \"zoom/x/y\" or \"zoom/x/y@extent\"."
                );
            }
            final int extent = parts.length == 2 ? Integer.parseInt(parts[1]) : 4096;
            final String[] tileBits = parts[0].split("/", 4);
            if (tileBits.length != 3) {
                throw new IllegalArgumentException(
                    "Invalid tile string [" + parts[0] + "]. Must be three integers in a form \"zoom/x/y\"."
                );
            }
            int z = GeoTileUtils.checkPrecisionRange(Integer.parseInt(tileBits[0]));
            final int tiles = 1 << z;
            int x = Integer.parseInt(tileBits[1]);
            int y = Integer.parseInt(tileBits[2]);
            if (x < 0 || y < 0 || x >= tiles || y >= tiles) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Zoom/X/Y combination is not valid: %d/%d/%d", z, x, y));
            }
            return new FeatureFactory(z, x, y, extent);
        });
    }
}
