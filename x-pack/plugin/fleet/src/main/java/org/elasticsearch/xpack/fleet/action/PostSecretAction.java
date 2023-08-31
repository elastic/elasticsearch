/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.action.ActionType;

public class PostSecretAction extends ActionType<PostSecretResponse> {

    public static final String NAME = "cluster:admin/fleet/secrets/post";

    public static final PostSecretAction INSTANCE = new PostSecretAction();

    private PostSecretAction() {
        super(NAME, PostSecretResponse::new);
    }
}
