/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.protocol.xpack.indexlifecycle.StartILMResponse;

public class StartILMAction extends Action<StartILMResponse> {
    public static final StartILMAction INSTANCE = new StartILMAction();
    public static final String NAME = "cluster:admin/ilm/start";

    protected StartILMAction() {
        super(NAME);
    }

    @Override
    public StartILMResponse newResponse() {
        return new StartILMResponse();
    }

}
