/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.protocol.xpack.indexlifecycle.PutOperationModeResponse;

public class PutOperationModeAction extends Action<PutOperationModeResponse> {
    public static final PutOperationModeAction INSTANCE = new PutOperationModeAction();
    public static final String NAME = "cluster:admin/ilm/operation_mode/set";

    protected PutOperationModeAction() {
        super(NAME);
    }

    @Override
    public PutOperationModeResponse newResponse() {
        return new PutOperationModeResponse();
    }

}
