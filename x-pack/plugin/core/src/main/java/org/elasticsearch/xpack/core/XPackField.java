/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core;

public final class XPackField {
    // These should be moved back to XPackPlugin once its moved to common
    /** Name constant for the security feature. */
    public static final String SECURITY = "security";
    /** Name constant for the monitoring feature. */
    public static final String MONITORING = "monitoring";
    /** Name constant for the watcher feature. */
    public static final String WATCHER = "watcher";
    /** Name constant for the graph feature. */
    public static final String GRAPH = "graph";
    /** Name constant for the machine learning feature. */
    public static final String MACHINE_LEARNING = "ml";
    /** Name constant for the Logstash feature. */
    public static final String LOGSTASH = "logstash";
    /** Name constant for the Beats feature. */
    public static final String BEATS = "beats";
    /** Name constant for the Deprecation API feature. */
    public static final String DEPRECATION = "deprecation";
    /** Name constant for the upgrade feature. */
    public static final String UPGRADE = "upgrade";
    // inside of YAML settings we still use xpack do not having handle issues with dashes
    public static final String SETTINGS_NAME = "xpack";
    /** Name constant for the eql feature. */
    public static final String EQL = "eql";
    /** Name constant for the esql feature. */
    public static final String ESQL = "esql";
    /** Name constant for the sql feature. */
    public static final String SQL = "sql";
    /** Name constant for the rollup feature. */
    public static final String ROLLUP = "rollup";
    /** Name constant for the index lifecycle feature. */
    public static final String INDEX_LIFECYCLE = "ilm";
    /** Name constant for the snapshot lifecycle management feature. */
    public static final String SNAPSHOT_LIFECYCLE = "slm";
    /** Name constant for the CCR feature. */
    public static final String CCR = "ccr";
    /** Name constant for the transform feature. */
    public static final String TRANSFORM = "transform";
    /** Name constant for flattened fields.
     *
     * @deprecated used for Backward Compatibility with 7.x only
     */
    @Deprecated
    public static final String FLATTENED = "flattened";
    @Deprecated
    /** Name constant for the vectors feature. */
    public static final String VECTORS = "vectors";
    /** Name constant for the voting-only-node feature. */
    public static final String VOTING_ONLY = "voting_only";
    /** Name constant for the frozen index feature. */
    public static final String FROZEN_INDICES = "frozen_indices";
    /** Name constant for spatial features. */
    public static final String SPATIAL = "spatial";
    /** Name constant for the analytics plugin. */
    public static final String ANALYTICS = "analytics";
    /** Name constant for the enrich plugin. */
    public static final String ENRICH = "enrich";
    /** Name constant for the searchable snapshots feature. */
    public static final String SEARCHABLE_SNAPSHOTS = "searchable_snapshots";
    /** Name constant for the data streams feature. */
    public static final String DATA_STREAMS = "data_streams";
    /** Name constant for the data lifecycle feature. */
    public static final String DATA_LIFECYCLE = "data_lifecycle";
    /** Name constant for the data tiers feature. */
    public static final String DATA_TIERS = "data_tiers";
    /** Name constant for the aggregate_metric plugin. */
    public static final String AGGREGATE_METRIC = "aggregate_metric";
    /** Name constant for the operator privileges feature. */
    public static final String OPERATOR_PRIVILEGES = "operator_privileges";
    /** Name constant for the archive feature. */
    public static final String ARCHIVE = "archive";
    /** Name constant for the health api feature. */
    public static final String HEALTH_API = "health_api";
    /** Name for Enterprise Search. */
    public static final String ENTERPRISE_SEARCH = "enterprise_search";
    public static final String REMOTE_CLUSTERS = "remote_clusters";

    /** Name constant for the redact processor feature. */
    public static final String REDACT_PROCESSOR = "redact_processor";

    private XPackField() {}

    public static String featureSettingPrefix(String featureName) {
        return XPackField.SETTINGS_NAME + "." + featureName;
    }
}
