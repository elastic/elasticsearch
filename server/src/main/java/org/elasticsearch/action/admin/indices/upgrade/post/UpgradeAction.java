/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.upgrade.post;

import org.elasticsearch.action.ActionType;

/**
 * Upgrade index/indices action.
 */
public class UpgradeAction extends ActionType<UpgradeResponse> {

    public static final UpgradeAction INSTANCE = new UpgradeAction();
    public static final String NAME = "indices:admin/upgrade";

    private UpgradeAction() {
        super(NAME, UpgradeResponse::new);
    }
}
