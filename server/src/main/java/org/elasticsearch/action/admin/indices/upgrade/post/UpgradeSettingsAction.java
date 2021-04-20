/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.upgrade.post;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

public class UpgradeSettingsAction extends ActionType<AcknowledgedResponse> {

    public static final UpgradeSettingsAction INSTANCE = new UpgradeSettingsAction();
    public static final String NAME = "internal:indices/admin/upgrade";

    private UpgradeSettingsAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

}
