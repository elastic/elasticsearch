/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;

/**
 * This action is for setting the enabled flag on a native or reserved user
 */
public class SetEnabledAction extends ActionType<ActionResponse.Empty> {

    public static final SetEnabledAction INSTANCE = new SetEnabledAction();
    public static final String NAME = "cluster:admin/xpack/security/user/set_enabled";

    private SetEnabledAction() {
        super(NAME, in -> ActionResponse.Empty.INSTANCE);
    }
}
