/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.action.compat;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction.Response;

public class UpdateTransformActionDeprecated extends ActionType<UpdateTransformAction.Response> {

    public static final UpdateTransformActionDeprecated INSTANCE = new UpdateTransformActionDeprecated();
    public static final String NAME = "cluster:admin/data_frame/update";

    private UpdateTransformActionDeprecated() {
        super(NAME, Response::new);
    }

}
