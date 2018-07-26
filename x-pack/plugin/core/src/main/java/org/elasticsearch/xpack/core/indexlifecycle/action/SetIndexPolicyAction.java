/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.protocol.xpack.indexlifecycle.SetIndexPolicyResponse;

public class SetIndexPolicyAction extends Action<SetIndexPolicyResponse> {

    public static final SetIndexPolicyAction INSTANCE = new SetIndexPolicyAction();
    public static final String NAME = "indices:admin/xpack/index_lifecycle/set_index_policy";

    protected SetIndexPolicyAction() {
        super(NAME);
    }

    @Override
    public SetIndexPolicyResponse newResponse() {
        return new SetIndexPolicyResponse();
    }
}
