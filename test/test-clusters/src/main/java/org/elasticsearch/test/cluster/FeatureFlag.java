/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster;

import org.elasticsearch.test.cluster.util.Version;

/**
 * Elasticsearch feature flags. Used in conjunction with {@link org.elasticsearch.test.cluster.local.LocalSpecBuilder#feature(FeatureFlag)}
 * to indicate that this feature is required and should be enabled when appropriate.
 */
public enum FeatureFlag {
    TIME_SERIES_MODE("es.index_mode_feature_flag_registered=true", Version.fromString("8.0.0"), null),
    FAILURE_STORE_ENABLED("es.failure_store_feature_flag_enabled=true", Version.fromString("8.12.0"), null),
    SUB_OBJECTS_AUTO_ENABLED("es.sub_objects_auto_feature_flag_enabled=true", Version.fromString("8.16.0"), null);

    public final String systemProperty;
    public final Version from;
    public final Version until;

    FeatureFlag(String systemProperty, Version from, Version until) {
        this.systemProperty = systemProperty;
        this.from = from;
        this.until = until;
    }
}
