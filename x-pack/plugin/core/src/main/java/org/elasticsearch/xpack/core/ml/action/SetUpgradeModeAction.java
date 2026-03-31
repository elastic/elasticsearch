/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.action.SetUpgradeModeActionRequest;

import java.io.IOException;

public class SetUpgradeModeAction extends ActionType<AcknowledgedResponse> {

    public static final SetUpgradeModeAction INSTANCE = new SetUpgradeModeAction();
    public static final String NAME = "cluster:admin/xpack/ml/upgrade_mode";

    private SetUpgradeModeAction() {
        super(NAME);
    }

    public static class Request extends SetUpgradeModeActionRequest {

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, boolean enabled) {
            super(masterNodeTimeout, ackTimeout, enabled);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }
    }
}
