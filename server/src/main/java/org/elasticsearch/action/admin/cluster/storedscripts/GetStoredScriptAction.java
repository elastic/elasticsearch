/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.action.ActionType;

public class GetStoredScriptAction {

    public static final ActionType<GetStoredScriptResponse> INSTANCE = new ActionType<>("cluster:admin/script/get");
    public static final String NAME = INSTANCE.name();

    private GetStoredScriptAction() {
        /* no instances */
    }
}
