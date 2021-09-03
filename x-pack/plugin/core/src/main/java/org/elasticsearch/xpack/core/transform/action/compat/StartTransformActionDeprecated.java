/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action.compat;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.xpack.core.transform.action.StartTransformAction;

public class StartTransformActionDeprecated  extends ActionType<StartTransformAction.Response> {

    public static final StartTransformActionDeprecated INSTANCE = new StartTransformActionDeprecated();
    public static final String NAME = "cluster:admin/data_frame/start";

    private StartTransformActionDeprecated() {
        super(NAME, StartTransformAction.Response::new);
    }

}
