/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.xpack.core.XPackField;

import java.util.Arrays;
import java.util.List;

/**
 * A base action for for usage of a feature plugin.
 *
 * This action is implemented by each feature plugin, bound to the public constants here. The
 * {@link XPackUsageAction} implementationn iterates over the {@link #ALL} list of actions to form
 * the complete usage result.
 */
public class XPackUsageFeatureAction extends Action<XPackUsageFeatureResponse> {

    private static final String BASE_NAME = "cluster:monitor/xpack/usage/";

    public static final XPackUsageFeatureAction SECURITY = new XPackUsageFeatureAction(XPackField.SECURITY);
    public static final XPackUsageFeatureAction MONITORING = new XPackUsageFeatureAction(XPackField.MONITORING);
    public static final XPackUsageFeatureAction WATCHER = new XPackUsageFeatureAction(XPackField.WATCHER);
    public static final XPackUsageFeatureAction GRAPH = new XPackUsageFeatureAction(XPackField.GRAPH);
    public static final XPackUsageFeatureAction MACHINE_LEARNING = new XPackUsageFeatureAction(XPackField.MACHINE_LEARNING);
    public static final XPackUsageFeatureAction LOGSTASH = new XPackUsageFeatureAction(XPackField.LOGSTASH);
    public static final XPackUsageFeatureAction SQL = new XPackUsageFeatureAction(XPackField.SQL);
    public static final XPackUsageFeatureAction ROLLUP = new XPackUsageFeatureAction(XPackField.ROLLUP);
    public static final XPackUsageFeatureAction INDEX_LIFECYCLE = new XPackUsageFeatureAction(XPackField.INDEX_LIFECYCLE);
    public static final XPackUsageFeatureAction CCR = new XPackUsageFeatureAction(XPackField.CCR);
    public static final XPackUsageFeatureAction DATA_FRAME = new XPackUsageFeatureAction(XPackField.DATA_FRAME);
    public static final XPackUsageFeatureAction VECTORS = new XPackUsageFeatureAction(XPackField.VECTORS);

    public static final List<XPackUsageFeatureAction> ALL = Arrays.asList(
        SECURITY, MONITORING, WATCHER, GRAPH, MACHINE_LEARNING, LOGSTASH, SQL, ROLLUP, INDEX_LIFECYCLE, CCR, DATA_FRAME, VECTORS
    );

    private XPackUsageFeatureAction(String name) {
        super(BASE_NAME + name);
    }

    @Override
    public XPackUsageFeatureResponse newResponse() {
        return new XPackUsageFeatureResponse();
    }

    @Override
    public String toString() {
        return "Action [" + name() + "]";
    }
}
