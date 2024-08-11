/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.core.Nullable;

/**
 * Provides the global retention configuration for data stream lifecycle as defined in the settings.
 */
public class DataStreamGlobalRetentionProvider {

    private final DataStreamFactoryRetention factoryRetention;

    public DataStreamGlobalRetentionProvider(DataStreamFactoryRetention factoryRetention) {
        this.factoryRetention = factoryRetention;
    }

    /**
     * Return the global retention configuration as defined in the settings. If both settings are null, it returns null.
     */
    @Nullable
    public DataStreamGlobalRetention provide() {
        if (factoryRetention.isDefined() == false) {
            return null;
        }
        return new DataStreamGlobalRetention(factoryRetention.getDefaultRetention(), factoryRetention.getMaxRetention());
    }
}
