/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.PluginsService;

/**
 * Holds the factory retention configuration. Factory retention is the global retention configuration meant to be
 * used if a user hasn't provided other retention configuration via {@link DataStreamGlobalRetention} metadata in the
 * cluster state.
 * @deprecated This interface is deprecated, please use {@link DataStreamGlobalRetentionSettings}.
 */
@Deprecated
public interface DataStreamFactoryRetention {

    @Nullable
    TimeValue getMaxRetention();

    @Nullable
    TimeValue getDefaultRetention();

    /**
     * @return true, if at least one of the two settings is not null, false otherwise.
     */
    default boolean isDefined() {
        return getMaxRetention() != null || getDefaultRetention() != null;
    }

    /**
     * Applies any post constructor initialisation, for example, listening to cluster setting changes.
     */
    void init(ClusterSettings clusterSettings);

    /**
     * Loads a single instance of a DataStreamFactoryRetention from the {@link PluginsService} and finalises the
     * initialisation by calling {@link DataStreamFactoryRetention#init(ClusterSettings)}
     */
    static DataStreamFactoryRetention load(PluginsService pluginsService, ClusterSettings clusterSettings) {
        DataStreamFactoryRetention factoryRetention = pluginsService.loadSingletonServiceProvider(
            DataStreamFactoryRetention.class,
            DataStreamFactoryRetention::emptyFactoryRetention
        );
        factoryRetention.init(clusterSettings);
        return factoryRetention;
    }

    /**
     * Returns empty factory global retention settings.
     */
    static DataStreamFactoryRetention emptyFactoryRetention() {
        return new DataStreamFactoryRetention() {

            @Override
            public TimeValue getMaxRetention() {
                return null;
            }

            @Override
            public TimeValue getDefaultRetention() {
                return null;
            }

            @Override
            public void init(ClusterSettings clusterSettings) {

            }
        };
    }
}
