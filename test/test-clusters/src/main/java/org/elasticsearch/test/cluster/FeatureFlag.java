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
    LOGS_STREAM("es.logs_stream_feature_flag_enabled=true", Version.fromString("9.1.0"), null),
    SYNTHETIC_VECTORS("es.mapping_synthetic_vectors=true", Version.fromString("9.2.0"), null),
    INDEX_DIMENSIONS_TSID_OPTIMIZATION_FEATURE_FLAG(
        "es.index_dimensions_tsid_optimization_feature_flag_enabled=true",
        Version.fromString("9.2.0"),
        null
    ),

    RANDOM_SAMPLING("es.random_sampling_feature_flag_enabled=true", Version.fromString("9.2.0"), null),
    TSDB_NO_SEQNO("es.tsdb_no_tsbd_feature_flag_enabled=true", Version.fromString("9.4.0"), null),
    PROMETHEUS_FEATURE_FLAG("es.prometheus_feature_flag_enabled=true", Version.fromString("9.4.0"), null),
    SLICE_INDEXING("es.slice_indexing_feature_flag_enabled=true", Version.fromString("9.5.0"), null),
    ESQL_EXTERNAL_DATASOURCES_LOCAL("es.esql_external_datasources_local_feature_flag_enabled=true", Version.fromString("9.5.0"), null),
    ESQL_EXTERNAL_DATASOURCES_HTTP("es.esql_external_datasources_http_feature_flag_enabled=true", Version.fromString("9.5.0"), null),
    ESQL_EXTERNAL_ORC("es.esql_external_orc_feature_flag_enabled=true", Version.fromString("9.5.0"), null),
    ESQL_EXTERNAL_GCS("es.esql_external_gcs_feature_flag_enabled=true", Version.fromString("9.5.0"), null),
    ESQL_EXTERNAL_AZURE("es.esql_external_azure_feature_flag_enabled=true", Version.fromString("9.5.0"), null),
    ESQL_EXTERNAL_PARQUET_RS("es.esql_external_parquet_rs_feature_flag_enabled=true", Version.fromString("9.5.0"), null);

    public final String systemProperty;
    public final Version from;
    public final Version until;

    FeatureFlag(String systemProperty, Version from, Version until) {
        this.systemProperty = systemProperty;
        this.from = from;
        this.until = until;
    }
}
