/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.recovery;

import org.elasticsearch.action.ActionType;

/**
 * Recovery information action
 */
public class RecoveryAction extends ActionType<RecoveryResponse> {

    public static final RecoveryAction INSTANCE = new RecoveryAction();
    public static final String NAME = "indices:monitor/recovery";

    private RecoveryAction() {
        super(NAME, RecoveryResponse::new);
    }
}
