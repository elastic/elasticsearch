/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.frozen.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.protocol.xpack.frozen.FreezeResponse;

public class FreezeIndexAction extends ActionType<FreezeResponse> {

    public static final FreezeIndexAction INSTANCE = new FreezeIndexAction();
    public static final String NAME = "indices:admin/freeze";

    private FreezeIndexAction() {
        super(NAME, FreezeResponse::new);
    }
}
