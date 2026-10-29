/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.xpack.core.esql.DataSourceRequestInfo;

public final class MockDataSourceRequest extends ActionRequest implements DataSourceRequestInfo {
    private final String actionName;
    private final String[] names;

    public MockDataSourceRequest(String actionName, String... names) {
        this.actionName = actionName;
        this.names = names;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public String[] dataSourceNames() {
        return names;
    }

    @Override
    public String dataSourceClusterActionName() {
        return actionName;
    }
}
