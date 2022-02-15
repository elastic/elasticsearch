/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.features;

import org.elasticsearch.action.ActionType;

/** Action for resetting feature states, mostly meaning system indices */
public class ResetFeatureStateAction extends ActionType<ResetFeatureStateResponse> {

    public static final ResetFeatureStateAction INSTANCE = new ResetFeatureStateAction();
    public static final String NAME = "cluster:admin/features/reset";

    private ResetFeatureStateAction() {
        super(NAME, ResetFeatureStateResponse::new);
    }
}
