/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.xpack.core.indexlifecycle.ExplainLifecycleResponse;

public class ExplainLifecycleAction extends Action<ExplainLifecycleResponse> {
    public static final ExplainLifecycleAction INSTANCE = new ExplainLifecycleAction();
    public static final String NAME = "indices:admin/ilm/explain";

    protected ExplainLifecycleAction() {
        super(NAME);
    }

    @Override
    public ExplainLifecycleResponse newResponse() {
        return new ExplainLifecycleResponse();
    }

}
