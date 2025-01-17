/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

public class SetTransformUpgradeModeAction extends ActionType<AcknowledgedResponse> {
    public static final SetTransformUpgradeModeAction INSTANCE = new SetTransformUpgradeModeAction();
    public static final String NAME = "cluster:admin/transform/upgrade_mode";

    private SetTransformUpgradeModeAction() {
        super(NAME);
    }
}
