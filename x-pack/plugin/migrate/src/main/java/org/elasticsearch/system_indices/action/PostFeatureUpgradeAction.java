/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.action;

import org.elasticsearch.action.ActionType;

/**
 * Action for beginning a system feature upgrade
 */
public class PostFeatureUpgradeAction extends ActionType<PostFeatureUpgradeResponse> {

    public static final PostFeatureUpgradeAction INSTANCE = new PostFeatureUpgradeAction();
    public static final String NAME = "cluster:admin/migration/post_system_feature";

    private PostFeatureUpgradeAction() {
        super(NAME);
    }
}
