/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionType;

/**
 * ActionType to create a SAML Response in the context of IDP initiated SSO for a given SP
 */
public class SamlInitiateSingleSignOnAction extends ActionType<SamlInitiateSingleSignOnResponse> {

    public static final String NAME = "cluster:admin/idp/saml/init";
    public static final SamlInitiateSingleSignOnAction INSTANCE = new SamlInitiateSingleSignOnAction();

    private SamlInitiateSingleSignOnAction() {
        super(NAME, SamlInitiateSingleSignOnResponse::new);
    }
}
