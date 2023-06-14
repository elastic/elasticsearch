/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster;

import org.elasticsearch.test.cluster.util.Version;

/**
 * Elasticsearch feature flags. Used in conjunction with {@link org.elasticsearch.test.cluster.local.LocalSpecBuilder#feature(FeatureFlag)}
 * to indicate that this feature is required and should be enabled when appropriate.
 */
public enum FeatureFlag {
    TIME_SERIES_MODE("es.index_mode_feature_flag_registered=true", Version.fromString("8.0.0"), null),
    NEW_RCS_MODE("es.untrusted_remote_cluster_feature_flag_registered=true", Version.fromString("8.5.0"), null),
    DLM_ENABLED("es.dlm_feature_flag_enabled=true", Version.fromString("8.8.0"), null),
    SYNONYMS_ENABLED("es.synonyms_feature_flag_enabled=true", Version.fromString("8.9.0"), null),
    QUERY_RULES_ENABLED("es.query_rules_feature_flag_enabled=true", Version.fromString("8.9.0"), null);

    public final String systemProperty;
    public final Version from;
    public final Version until;

    FeatureFlag(String systemProperty, Version from, Version until) {
        this.systemProperty = systemProperty;
        this.from = from;
        this.until = until;
    }
}
