/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.secrets.action;

import org.elasticsearch.action.ActionType;

public class DeleteConnectorSecretAction {

    public static final String NAME = "cluster:admin/xpack/connector/secret/delete";

    public static final ActionType<DeleteConnectorSecretResponse> INSTANCE = new ActionType<>(NAME);

    private DeleteConnectorSecretAction() {/* no instances */}
}
