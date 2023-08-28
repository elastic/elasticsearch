/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.action.ActionType;

public class DeleteSecretAction extends ActionType<DeleteSecretResponse> {

    public static final String NAME = "cluster:admin/fleet/secrets/delete";

    public static final DeleteSecretAction INSTANCE = new DeleteSecretAction();

    private DeleteSecretAction() {
        super(NAME, DeleteSecretResponse::new);
    }
}
