/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.protocol.xpack.indexlifecycle.SetIndexLifecyclePolicyResponse;

public class SetIndexLifecyclePolicyAction extends Action<SetIndexLifecyclePolicyResponse> {

    public static final SetIndexLifecyclePolicyAction INSTANCE = new SetIndexLifecyclePolicyAction();
    public static final String NAME = "indices:admin/xpack/index_lifecycle/set_index_policy";

    protected SetIndexLifecyclePolicyAction() {
        super(NAME);
    }

    @Override
    public SetIndexLifecyclePolicyResponse newResponse() {
        return new SetIndexLifecyclePolicyResponse();
    }
}
