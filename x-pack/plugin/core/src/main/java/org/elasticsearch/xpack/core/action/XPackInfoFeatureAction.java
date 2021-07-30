/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.xpack.core.XPackField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A base action for info about a feature plugin.
 *
 * This action is implemented by each feature plugin, bound to the public constants here. The
 * {@link XPackInfoAction} implementation iterates over the {@link #ALL} list of actions to form
 * the complete info result.
 */
public class XPackInfoFeatureAction extends ActionType<XPackInfoFeatureResponse> {

    private static final String BASE_NAME = "cluster:monitor/xpack/info/";

    public static final XPackInfoFeatureAction SECURITY = new XPackInfoFeatureAction(XPackField.SECURITY);
    public static final XPackInfoFeatureAction MONITORING = new XPackInfoFeatureAction(XPackField.MONITORING);
    public static final XPackInfoFeatureAction WATCHER = new XPackInfoFeatureAction(XPackField.WATCHER);
    public static final XPackInfoFeatureAction GRAPH = new XPackInfoFeatureAction(XPackField.GRAPH);
    public static final XPackInfoFeatureAction MACHINE_LEARNING = new XPackInfoFeatureAction(XPackField.MACHINE_LEARNING);
    public static final XPackInfoFeatureAction LOGSTASH = new XPackInfoFeatureAction(XPackField.LOGSTASH);
    public static final XPackInfoFeatureAction EQL = new XPackInfoFeatureAction(XPackField.EQL);
    public static final XPackInfoFeatureAction SQL = new XPackInfoFeatureAction(XPackField.SQL);
    public static final XPackInfoFeatureAction ROLLUP = new XPackInfoFeatureAction(XPackField.ROLLUP);
    public static final XPackInfoFeatureAction INDEX_LIFECYCLE = new XPackInfoFeatureAction(XPackField.INDEX_LIFECYCLE);
    public static final XPackInfoFeatureAction SNAPSHOT_LIFECYCLE = new XPackInfoFeatureAction(XPackField.SNAPSHOT_LIFECYCLE);
    public static final XPackInfoFeatureAction CCR = new XPackInfoFeatureAction(XPackField.CCR);
    public static final XPackInfoFeatureAction TRANSFORM = new XPackInfoFeatureAction(XPackField.TRANSFORM);
    public static final XPackInfoFeatureAction VOTING_ONLY = new XPackInfoFeatureAction(XPackField.VOTING_ONLY);
    public static final XPackInfoFeatureAction FROZEN_INDICES = new XPackInfoFeatureAction(XPackField.FROZEN_INDICES);
    public static final XPackInfoFeatureAction SPATIAL = new XPackInfoFeatureAction(XPackField.SPATIAL);
    public static final XPackInfoFeatureAction ANALYTICS = new XPackInfoFeatureAction(XPackField.ANALYTICS);
    public static final XPackInfoFeatureAction ENRICH = new XPackInfoFeatureAction(XPackField.ENRICH);
    public static final XPackInfoFeatureAction SEARCHABLE_SNAPSHOTS = new XPackInfoFeatureAction(XPackField.SEARCHABLE_SNAPSHOTS);
    public static final XPackInfoFeatureAction DATA_STREAMS = new XPackInfoFeatureAction(XPackField.DATA_STREAMS);
    public static final XPackInfoFeatureAction DATA_TIERS = new XPackInfoFeatureAction(XPackField.DATA_TIERS);
    public static final XPackInfoFeatureAction AGGREGATE_METRIC = new XPackInfoFeatureAction(XPackField.AGGREGATE_METRIC);

    public static final List<XPackInfoFeatureAction> ALL;
    static {
        final List<XPackInfoFeatureAction> actions = new ArrayList<>();
        actions.addAll(Arrays.asList(
            SECURITY, MONITORING, WATCHER, GRAPH, MACHINE_LEARNING, LOGSTASH, EQL, SQL, ROLLUP, INDEX_LIFECYCLE, SNAPSHOT_LIFECYCLE, CCR,
            TRANSFORM, VOTING_ONLY, FROZEN_INDICES, SPATIAL, ANALYTICS, ENRICH, DATA_STREAMS, SEARCHABLE_SNAPSHOTS, DATA_TIERS,
            AGGREGATE_METRIC
        ));
        ALL = Collections.unmodifiableList(actions);
    }

    private XPackInfoFeatureAction(String name) {
        super(BASE_NAME + name, XPackInfoFeatureResponse::new);
    }

    @Override
    public String toString() {
        return "ActionType [" + name() + "]";
    }
}
