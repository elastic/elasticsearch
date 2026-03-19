/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.xpack.core.esql.EsqlCursorActionNames;

public class EsqlDeleteCursorAction extends ActionType<AcknowledgedResponse> {

    public static final EsqlDeleteCursorAction INSTANCE = new EsqlDeleteCursorAction();
    public static final String NAME = EsqlCursorActionNames.ESQL_DELETE_CURSOR_ACTION_NAME;

    private EsqlDeleteCursorAction() {
        super(NAME);
    }
}
