/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.async;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

public class DeleteAsyncResultAction extends ActionType<AcknowledgedResponse> {
    public static final DeleteAsyncResultAction INSTANCE = new DeleteAsyncResultAction();
    public static final String NAME = "indices:data/read/async_search/delete";

    private DeleteAsyncResultAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }
}
