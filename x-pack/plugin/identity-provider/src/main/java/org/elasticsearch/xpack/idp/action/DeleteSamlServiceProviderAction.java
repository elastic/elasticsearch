/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionType;

/**
 * Action to remove a service provider from the IdP.
 */
public class DeleteSamlServiceProviderAction extends ActionType<DeleteSamlServiceProviderResponse> {

    public static final String NAME = "cluster:admin/idp/saml/sp/delete";
    public static final DeleteSamlServiceProviderAction INSTANCE = new DeleteSamlServiceProviderAction(NAME);

    public DeleteSamlServiceProviderAction(String name) {
        super(name, DeleteSamlServiceProviderResponse::new);
    }
}
