/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionType;

public class SamlValidateAuthnRequestAction extends ActionType<SamlValidateAuthnRequestResponse> {

    public static final String NAME = "cluster:admin/idp/saml/validate";
    public static final SamlValidateAuthnRequestAction INSTANCE = new SamlValidateAuthnRequestAction();

    private SamlValidateAuthnRequestAction() {
        super(NAME, SamlValidateAuthnRequestResponse::new);
    }
}
