/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.action.compat;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;

public class GetTransformActionDeprecated extends ActionType<GetTransformAction.Response> {

    public static final GetTransformActionDeprecated INSTANCE = new GetTransformActionDeprecated();
    public static final String NAME = "cluster:monitor/data_frame/get";

    private GetTransformActionDeprecated() {
        super(NAME, GetTransformAction.Response::new);
    }
}
