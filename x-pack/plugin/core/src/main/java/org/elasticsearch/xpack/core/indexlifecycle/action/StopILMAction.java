/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.protocol.xpack.indexlifecycle.StopILMResponse;

public class StopILMAction extends Action<StopILMResponse> {
    public static final StopILMAction INSTANCE = new StopILMAction();
    public static final String NAME = "cluster:admin/ilm/stop";

    protected StopILMAction() {
        super(NAME);
    }

    @Override
    public StopILMResponse newResponse() {
        return new StopILMResponse();
    }

}
