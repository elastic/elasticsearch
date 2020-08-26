/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.async;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.Writeable;

public class DeleteAsyncResultAction extends ActionType<AcknowledgedResponse> {
    public static final DeleteAsyncResultAction INSTANCE = new DeleteAsyncResultAction();
    public static final String NAME = "indices:data/read/async_search/delete";

    private DeleteAsyncResultAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    @Override
    public Writeable.Reader<AcknowledgedResponse> getResponseReader() {
        return AcknowledgedResponse::new;
    }

}
