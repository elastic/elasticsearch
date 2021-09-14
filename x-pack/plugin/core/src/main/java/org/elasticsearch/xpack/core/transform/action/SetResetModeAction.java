/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;


public class SetResetModeAction extends ActionType<AcknowledgedResponse> {

    public static final SetResetModeAction INSTANCE = new SetResetModeAction();
    public static final String NAME = "cluster:internal/xpack/transform/reset_mode";

    private SetResetModeAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

}
