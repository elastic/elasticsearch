/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A base action for for usage of a feature plugin.
 *
 * This action is implemented by each feature plugin, bound to the public constants here. The
 * {@link XPackUsageAction} implementationn iterates over the {@link #ALL} list of actions to form
 * the complete usage result.
 */
public class XPackUsageFeatureAction extends ActionType<XPackUsageFeatureResponse> {

    private static final String BASE_NAME = "cluster:monitor/xpack/usage/";

    public static final XPackUsageFeatureAction SECURITY = new XPackUsageFeatureAction(XPackField.SECURITY);
    public static final XPackUsageFeatureAction MONITORING = new XPackUsageFeatureAction(XPackField.MONITORING);
    public static final XPackUsageFeatureAction WATCHER = new XPackUsageFeatureAction(XPackField.WATCHER);
    public static final XPackUsageFeatureAction GRAPH = new XPackUsageFeatureAction(XPackField.GRAPH);
    public static final XPackUsageFeatureAction MACHINE_LEARNING = new XPackUsageFeatureAction(XPackField.MACHINE_LEARNING);
    public static final XPackUsageFeatureAction LOGSTASH = new XPackUsageFeatureAction(XPackField.LOGSTASH);
    public static final XPackUsageFeatureAction EQL = new XPackUsageFeatureAction(XPackField.EQL);
    public static final XPackUsageFeatureAction SQL = new XPackUsageFeatureAction(XPackField.SQL);
    public static final XPackUsageFeatureAction ROLLUP = new XPackUsageFeatureAction(XPackField.ROLLUP);
    public static final XPackUsageFeatureAction INDEX_LIFECYCLE = new XPackUsageFeatureAction(XPackField.INDEX_LIFECYCLE);
    public static final XPackUsageFeatureAction SNAPSHOT_LIFECYCLE = new XPackUsageFeatureAction(XPackField.SNAPSHOT_LIFECYCLE);
    public static final XPackUsageFeatureAction CCR = new XPackUsageFeatureAction(XPackField.CCR);
    public static final XPackUsageFeatureAction TRANSFORM = new XPackUsageFeatureAction(XPackField.TRANSFORM);
    public static final XPackUsageFeatureAction VECTORS = new XPackUsageFeatureAction(XPackField.VECTORS);
    public static final XPackUsageFeatureAction VOTING_ONLY = new XPackUsageFeatureAction(XPackField.VOTING_ONLY);
    public static final XPackUsageFeatureAction FROZEN_INDICES = new XPackUsageFeatureAction(XPackField.FROZEN_INDICES);
    public static final XPackUsageFeatureAction SPATIAL = new XPackUsageFeatureAction(XPackField.SPATIAL);
    public static final XPackUsageFeatureAction ANALYTICS = new XPackUsageFeatureAction(XPackField.ANALYTICS);
    public static final XPackUsageFeatureAction ENRICH = new XPackUsageFeatureAction(XPackField.ENRICH);
    public static final XPackUsageFeatureAction SEARCHABLE_SNAPSHOTS = new XPackUsageFeatureAction(XPackField.SEARCHABLE_SNAPSHOTS);
    public static final XPackUsageFeatureAction DATA_STREAMS = new XPackUsageFeatureAction(XPackField.DATA_STREAMS);

    public static final List<XPackUsageFeatureAction> ALL;
    static {
        final List<XPackUsageFeatureAction> actions = new ArrayList<>();
        actions.addAll(Arrays.asList(
            SECURITY, MONITORING, WATCHER, GRAPH, MACHINE_LEARNING, LOGSTASH, EQL, SQL, ROLLUP, INDEX_LIFECYCLE, SNAPSHOT_LIFECYCLE, CCR,
            TRANSFORM, VECTORS, VOTING_ONLY, FROZEN_INDICES, SPATIAL, ANALYTICS, DATA_STREAMS
        ));
        if (SearchableSnapshotsConstants.SEARCHABLE_SNAPSHOTS_FEATURE_ENABLED) {
            actions.add(SEARCHABLE_SNAPSHOTS);
        }
        ALL = Collections.unmodifiableList(actions);
    }

    private XPackUsageFeatureAction(String name) {
        super(BASE_NAME + name, XPackUsageFeatureResponse::new);
    }

    @Override
    public String toString() {
        return "ActionType [" + name() + "]";
    }
}
