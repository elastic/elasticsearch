/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.xpack.core.esql.EsqlStreamActionNames;

/**
 * Deletes a stored ES|QL stream and cancels the task that writes the results of a stream query to the stream system index if it is still
 * running, the counterpart of the async delete ({@code TransportDeleteAsyncResultAction#TYPE}). For now nothing is stored, so every id is
 * not found.
 */
public class EsqlStreamDeleteAction extends ActionType<AcknowledgedResponse> {

    public static final EsqlStreamDeleteAction INSTANCE = new EsqlStreamDeleteAction();

    public static final String NAME = EsqlStreamActionNames.ESQL_STREAM_DELETE_ACTION_NAME;

    private EsqlStreamDeleteAction() {
        super(NAME);
    }
}
