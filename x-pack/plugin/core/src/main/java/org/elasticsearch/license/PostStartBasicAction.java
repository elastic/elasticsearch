/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionType;

public class PostStartBasicAction extends ActionType<PostStartBasicResponse> {

    public static final PostStartBasicAction INSTANCE = new PostStartBasicAction();
    public static final String NAME = "cluster:admin/xpack/license/start_basic";

    private PostStartBasicAction() {
        super(NAME);
    }
}
