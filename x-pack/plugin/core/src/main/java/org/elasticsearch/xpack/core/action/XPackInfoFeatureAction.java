/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.xpack.core.XPackField;

import java.util.List;

/**
 * A base action for info about a feature plugin.
 *
 * This action is implemented by each feature plugin, bound to the public constants here. The
 * {@link XPackInfoAction} implementation iterates over the {@link #ALL} list of actions to form
 * the complete info result.
 */
public class XPackInfoFeatureAction {

    private static final String BASE_NAME = "cluster:monitor/xpack/info/";

    public static final ActionType<XPackInfoFeatureResponse> SECURITY = xpackInfoFeatureAction(XPackField.SECURITY);
    public static final ActionType<XPackInfoFeatureResponse> MONITORING = xpackInfoFeatureAction(XPackField.MONITORING);
    public static final ActionType<XPackInfoFeatureResponse> WATCHER = xpackInfoFeatureAction(XPackField.WATCHER);
    public static final ActionType<XPackInfoFeatureResponse> GRAPH = xpackInfoFeatureAction(XPackField.GRAPH);
    public static final ActionType<XPackInfoFeatureResponse> MACHINE_LEARNING = xpackInfoFeatureAction(XPackField.MACHINE_LEARNING);
    public static final ActionType<XPackInfoFeatureResponse> LOGSTASH = xpackInfoFeatureAction(XPackField.LOGSTASH);
    public static final ActionType<XPackInfoFeatureResponse> EQL = xpackInfoFeatureAction(XPackField.EQL);
    public static final ActionType<XPackInfoFeatureResponse> ESQL = xpackInfoFeatureAction(XPackField.ESQL);
    public static final ActionType<XPackInfoFeatureResponse> SQL = xpackInfoFeatureAction(XPackField.SQL);
    public static final ActionType<XPackInfoFeatureResponse> ROLLUP = xpackInfoFeatureAction(XPackField.ROLLUP);
    public static final ActionType<XPackInfoFeatureResponse> INDEX_LIFECYCLE = xpackInfoFeatureAction(XPackField.INDEX_LIFECYCLE);
    public static final ActionType<XPackInfoFeatureResponse> SNAPSHOT_LIFECYCLE = xpackInfoFeatureAction(XPackField.SNAPSHOT_LIFECYCLE);
    public static final ActionType<XPackInfoFeatureResponse> CCR = xpackInfoFeatureAction(XPackField.CCR);
    public static final ActionType<XPackInfoFeatureResponse> TRANSFORM = xpackInfoFeatureAction(XPackField.TRANSFORM);
    public static final ActionType<XPackInfoFeatureResponse> VOTING_ONLY = xpackInfoFeatureAction(XPackField.VOTING_ONLY);
    public static final ActionType<XPackInfoFeatureResponse> FROZEN_INDICES = xpackInfoFeatureAction(XPackField.FROZEN_INDICES);
    public static final ActionType<XPackInfoFeatureResponse> SPATIAL = xpackInfoFeatureAction(XPackField.SPATIAL);
    public static final ActionType<XPackInfoFeatureResponse> ANALYTICS = xpackInfoFeatureAction(XPackField.ANALYTICS);
    public static final ActionType<XPackInfoFeatureResponse> ENRICH = xpackInfoFeatureAction(XPackField.ENRICH);
    public static final ActionType<XPackInfoFeatureResponse> SEARCHABLE_SNAPSHOTS = xpackInfoFeatureAction(XPackField.SEARCHABLE_SNAPSHOTS);
    public static final ActionType<XPackInfoFeatureResponse> DATA_STREAMS = xpackInfoFeatureAction(XPackField.DATA_STREAMS);
    public static final ActionType<XPackInfoFeatureResponse> DATA_TIERS = xpackInfoFeatureAction(XPackField.DATA_TIERS);
    public static final ActionType<XPackInfoFeatureResponse> AGGREGATE_METRIC = xpackInfoFeatureAction(XPackField.AGGREGATE_METRIC);
    public static final ActionType<XPackInfoFeatureResponse> ARCHIVE = xpackInfoFeatureAction(XPackField.ARCHIVE);
    public static final ActionType<XPackInfoFeatureResponse> ENTERPRISE_SEARCH = xpackInfoFeatureAction(XPackField.ENTERPRISE_SEARCH);
    public static final ActionType<XPackInfoFeatureResponse> UNIVERSAL_PROFILING = xpackInfoFeatureAction(XPackField.UNIVERSAL_PROFILING);
    public static final ActionType<XPackInfoFeatureResponse> LOGSDB = xpackInfoFeatureAction(XPackField.LOGSDB);

    public static final List<ActionType<XPackInfoFeatureResponse>> ALL = List.of(
        SECURITY,
        MONITORING,
        WATCHER,
        GRAPH,
        MACHINE_LEARNING,
        LOGSTASH,
        EQL,
        ESQL,
        SQL,
        ROLLUP,
        INDEX_LIFECYCLE,
        SNAPSHOT_LIFECYCLE,
        CCR,
        TRANSFORM,
        VOTING_ONLY,
        FROZEN_INDICES,
        SPATIAL,
        ANALYTICS,
        ENRICH,
        DATA_STREAMS,
        SEARCHABLE_SNAPSHOTS,
        DATA_TIERS,
        AGGREGATE_METRIC,
        ARCHIVE,
        ENTERPRISE_SEARCH,
        UNIVERSAL_PROFILING,
        LOGSDB
    );

    public static ActionType<XPackInfoFeatureResponse> xpackInfoFeatureAction(String suffix) {
        return new ActionType<>(BASE_NAME + suffix);
    }

    private XPackInfoFeatureAction() {/* no instances */}
}
