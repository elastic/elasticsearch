/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.xpack.core.XPackField;

import java.util.List;

/**
 * A base action for usage of a feature plugin.
 *
 * This action is implemented by each feature plugin, bound to the public constants here. The
 * {@link XPackUsageAction} implementation iterates over the {@link #ALL} list of actions to form
 * the complete usage result.
 */
public final class XPackUsageFeatureAction {

    private XPackUsageFeatureAction() {/* no instances */}

    private static final String BASE_NAME = "cluster:monitor/xpack/usage/";

    public static final ActionType<XPackUsageFeatureResponse> SECURITY = xpackUsageFeatureAction(XPackField.SECURITY);
    public static final ActionType<XPackUsageFeatureResponse> MONITORING = xpackUsageFeatureAction(XPackField.MONITORING);
    public static final ActionType<XPackUsageFeatureResponse> WATCHER = xpackUsageFeatureAction(XPackField.WATCHER);
    public static final ActionType<XPackUsageFeatureResponse> GRAPH = xpackUsageFeatureAction(XPackField.GRAPH);
    public static final ActionType<XPackUsageFeatureResponse> MACHINE_LEARNING = xpackUsageFeatureAction(XPackField.MACHINE_LEARNING);
    public static final ActionType<XPackUsageFeatureResponse> INFERENCE = xpackUsageFeatureAction(XPackField.INFERENCE);
    public static final ActionType<XPackUsageFeatureResponse> LOGSTASH = xpackUsageFeatureAction(XPackField.LOGSTASH);
    public static final ActionType<XPackUsageFeatureResponse> EQL = xpackUsageFeatureAction(XPackField.EQL);
    public static final ActionType<XPackUsageFeatureResponse> ESQL = xpackUsageFeatureAction(XPackField.ESQL);
    public static final ActionType<XPackUsageFeatureResponse> SQL = xpackUsageFeatureAction(XPackField.SQL);
    public static final ActionType<XPackUsageFeatureResponse> ROLLUP = xpackUsageFeatureAction(XPackField.ROLLUP);
    public static final ActionType<XPackUsageFeatureResponse> INDEX_LIFECYCLE = xpackUsageFeatureAction(XPackField.INDEX_LIFECYCLE);
    public static final ActionType<XPackUsageFeatureResponse> SNAPSHOT_LIFECYCLE = xpackUsageFeatureAction(XPackField.SNAPSHOT_LIFECYCLE);
    public static final ActionType<XPackUsageFeatureResponse> CCR = xpackUsageFeatureAction(XPackField.CCR);
    public static final ActionType<XPackUsageFeatureResponse> TRANSFORM = xpackUsageFeatureAction(XPackField.TRANSFORM);
    public static final ActionType<XPackUsageFeatureResponse> VOTING_ONLY = xpackUsageFeatureAction(XPackField.VOTING_ONLY);
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT) // Remove this: it is unused in v9 but needed for mixed v8/v9 clusters
    public static final ActionType<XPackUsageFeatureResponse> FROZEN_INDICES = xpackUsageFeatureAction(XPackField.FROZEN_INDICES);
    public static final ActionType<XPackUsageFeatureResponse> SPATIAL = xpackUsageFeatureAction(XPackField.SPATIAL);
    public static final ActionType<XPackUsageFeatureResponse> ANALYTICS = xpackUsageFeatureAction(XPackField.ANALYTICS);
    public static final ActionType<XPackUsageFeatureResponse> ENRICH = xpackUsageFeatureAction(XPackField.ENRICH);
    public static final ActionType<XPackUsageFeatureResponse> SEARCHABLE_SNAPSHOTS = xpackUsageFeatureAction(
        XPackField.SEARCHABLE_SNAPSHOTS
    );
    public static final ActionType<XPackUsageFeatureResponse> DATA_STREAMS = xpackUsageFeatureAction(XPackField.DATA_STREAMS);
    public static final ActionType<XPackUsageFeatureResponse> DATA_STREAM_LIFECYCLE = xpackUsageFeatureAction(
        XPackField.DATA_STREAM_LIFECYCLE
    );
    public static final ActionType<XPackUsageFeatureResponse> DATA_TIERS = xpackUsageFeatureAction(XPackField.DATA_TIERS);
    public static final ActionType<XPackUsageFeatureResponse> AGGREGATE_METRIC = xpackUsageFeatureAction(XPackField.AGGREGATE_METRIC);
    public static final ActionType<XPackUsageFeatureResponse> ARCHIVE = xpackUsageFeatureAction(XPackField.ARCHIVE);
    public static final ActionType<XPackUsageFeatureResponse> HEALTH = xpackUsageFeatureAction(XPackField.HEALTH_API);
    public static final ActionType<XPackUsageFeatureResponse> REMOTE_CLUSTERS = xpackUsageFeatureAction(XPackField.REMOTE_CLUSTERS);
    public static final ActionType<XPackUsageFeatureResponse> ENTERPRISE_SEARCH = xpackUsageFeatureAction(XPackField.ENTERPRISE_SEARCH);
    public static final ActionType<XPackUsageFeatureResponse> UNIVERSAL_PROFILING = xpackUsageFeatureAction(XPackField.UNIVERSAL_PROFILING);
    public static final ActionType<XPackUsageFeatureResponse> LOGSDB = xpackUsageFeatureAction(XPackField.LOGSDB);

    static final List<ActionType<XPackUsageFeatureResponse>> ALL = List.of(
        AGGREGATE_METRIC,
        ANALYTICS,
        CCR,
        DATA_STREAMS,
        DATA_STREAM_LIFECYCLE,
        DATA_TIERS,
        EQL,
        ESQL,
        GRAPH,
        INDEX_LIFECYCLE,
        INFERENCE,
        LOGSTASH,
        MACHINE_LEARNING,
        MONITORING,
        ROLLUP,
        SEARCHABLE_SNAPSHOTS,
        SECURITY,
        SNAPSHOT_LIFECYCLE,
        SPATIAL,
        SQL,
        TRANSFORM,
        VOTING_ONLY,
        WATCHER,
        ARCHIVE,
        HEALTH,
        REMOTE_CLUSTERS,
        ENTERPRISE_SEARCH,
        UNIVERSAL_PROFILING,
        LOGSDB
    );

    public static ActionType<XPackUsageFeatureResponse> xpackUsageFeatureAction(String suffix) {
        return new ActionType<>(BASE_NAME + suffix);
    }
}
