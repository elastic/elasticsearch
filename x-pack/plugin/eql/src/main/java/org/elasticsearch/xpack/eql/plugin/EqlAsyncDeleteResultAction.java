/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.plugin;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.eql.EqlAsyncActionNames;

public class EqlAsyncDeleteResultAction extends ActionType<AcknowledgedResponse> {
    public static final EqlAsyncDeleteResultAction INSTANCE = new EqlAsyncDeleteResultAction();

    private EqlAsyncDeleteResultAction() {
        super(EqlAsyncActionNames.EQL_ASYNC_DELETE_RESULT_ACTION_NAME, AcknowledgedResponse::new);
    }

    @Override
    public Writeable.Reader<AcknowledgedResponse> getResponseReader() {
        return AcknowledgedResponse::new;
    }
}
