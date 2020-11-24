/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.example.actions;

import org.elasticsearch.action.ActionType;

public class GetActionsAction extends ActionType<GetActionsResponse> {

    public static final String NAME = "cluster:monitor/test/get_actions";
    public static final GetActionsAction INSTANCE = new GetActionsAction();

    public GetActionsAction() {
        super(NAME, GetActionsResponse::new);
    }
}
