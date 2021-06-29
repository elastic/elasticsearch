/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.action.ActionType;

public class GetScriptLanguageAction extends ActionType<GetScriptLanguageResponse> {
    public static final GetScriptLanguageAction INSTANCE = new GetScriptLanguageAction();
    public static final String NAME = "cluster:admin/script_language/get";

    private GetScriptLanguageAction() {
        super(NAME, GetScriptLanguageResponse::new);
    }
}
