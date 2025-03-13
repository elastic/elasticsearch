/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionType;

public class XPackUsageAction {

    public static final String NAME = "cluster:monitor/xpack/usage";
    public static final ActionType<XPackUsageResponse> INSTANCE = new ActionType<>(NAME);

    private XPackUsageAction() {/* no instances */}
}
