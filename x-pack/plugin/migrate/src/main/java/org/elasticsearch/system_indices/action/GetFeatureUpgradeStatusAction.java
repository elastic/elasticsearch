/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.action;

import org.elasticsearch.action.ActionType;

/**
 * Action for getting a feature upgrade status.
 */
public class GetFeatureUpgradeStatusAction extends ActionType<GetFeatureUpgradeStatusResponse> {

    public static final GetFeatureUpgradeStatusAction INSTANCE = new GetFeatureUpgradeStatusAction();
    public static final String NAME = "cluster:admin/migration/get_system_feature";

    private GetFeatureUpgradeStatusAction() {
        super(NAME);
    }
}
