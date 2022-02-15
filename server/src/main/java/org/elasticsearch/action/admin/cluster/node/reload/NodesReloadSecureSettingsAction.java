/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.reload;

import org.elasticsearch.action.ActionType;

public class NodesReloadSecureSettingsAction extends ActionType<NodesReloadSecureSettingsResponse> {

    public static final NodesReloadSecureSettingsAction INSTANCE = new NodesReloadSecureSettingsAction();
    public static final String NAME = "cluster:admin/nodes/reload_secure_settings";

    private NodesReloadSecureSettingsAction() {
        super(NAME, NodesReloadSecureSettingsResponse::new);
    }
}
