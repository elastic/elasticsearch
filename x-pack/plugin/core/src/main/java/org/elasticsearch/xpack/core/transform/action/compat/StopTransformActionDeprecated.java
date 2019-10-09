/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.action.compat;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.xpack.core.transform.action.StopTransformAction;

public class StopTransformActionDeprecated extends ActionType<StopTransformAction.Response> {

    public static final StopTransformActionDeprecated INSTANCE = new StopTransformActionDeprecated();
    public static final String NAME = "cluster:admin/data_frame/stop";

    private StopTransformActionDeprecated() {
        super(NAME, StopTransformAction.Response::new);
    }

}
