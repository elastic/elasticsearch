/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

public class StartILMAction extends ActionType<AcknowledgedResponse> {
    public static final StartILMAction INSTANCE = new StartILMAction();
    public static final String NAME = "cluster:admin/ilm/start";

    protected StartILMAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
