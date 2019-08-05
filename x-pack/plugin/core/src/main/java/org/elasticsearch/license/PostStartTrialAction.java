/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionType;

public class PostStartTrialAction extends ActionType<PostStartTrialResponse> {

    public static final PostStartTrialAction INSTANCE = new PostStartTrialAction();
    public static final String NAME = "cluster:admin/xpack/license/start_trial";

    private PostStartTrialAction() {
        super(NAME, PostStartTrialResponse::new);
    }
}
