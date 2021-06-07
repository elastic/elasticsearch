/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionType;

public class PutSamlServiceProviderAction extends ActionType<PutSamlServiceProviderResponse> {

    public static final String NAME = "cluster:admin/idp/saml/sp/put";
    public static final PutSamlServiceProviderAction INSTANCE = new PutSamlServiceProviderAction(NAME);

    public PutSamlServiceProviderAction(String name) {
        super(name, PutSamlServiceProviderResponse::new);
    }
}
