/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionType;

public class EsqlProjectRoutingAction extends ActionType<EsqlProjectRoutingResponse> {
    public static final String NAME = "cluster:monitor/xpack/esql/project_routing";
    public static final EsqlProjectRoutingAction INSTANCE = new EsqlProjectRoutingAction(NAME);

    public EsqlProjectRoutingAction(String name) {
        super(NAME);
    }
}
