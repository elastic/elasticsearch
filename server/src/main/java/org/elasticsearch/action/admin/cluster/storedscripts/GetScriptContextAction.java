/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.action.ActionType;

public class GetScriptContextAction extends ActionType<GetScriptContextResponse> {

    public static final GetScriptContextAction INSTANCE = new GetScriptContextAction();
    public static final String NAME = "cluster:admin/script_context/get";

    private GetScriptContextAction() {
        super(NAME, GetScriptContextResponse::new);
    }
}
