/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 *
 */
package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.protocol.xpack.indexlifecycle.MoveToStepResponse;

public class MoveToStepAction extends Action<MoveToStepResponse> {
    public static final MoveToStepAction INSTANCE = new MoveToStepAction();
    public static final String NAME = "indices:admin/ilm/move_to_step";

    protected MoveToStepAction() {
        super(NAME);
    }

    @Override
    public MoveToStepResponse newResponse() {
        return new MoveToStepResponse();
    }
}
