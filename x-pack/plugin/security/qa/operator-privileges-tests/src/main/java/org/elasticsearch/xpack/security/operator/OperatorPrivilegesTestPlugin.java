/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.security.operator.actions.RestGetActionsAction;

import java.util.List;

public class OperatorPrivilegesTestPlugin extends Plugin implements ActionPlugin {

    @Override
    public List<RestHandler> getRestHandlers(RestHandlerParameters parameters) {
        return List.of(new RestGetActionsAction());
    }
}
