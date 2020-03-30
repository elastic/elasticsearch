/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionType;

public class SamlMetadataAction extends ActionType<SamlMetadataResponse> {

    public static final String NAME = "cluster:admin/idp/saml/metadata";
    public static final SamlMetadataAction INSTANCE = new SamlMetadataAction();

    private SamlMetadataAction() {
        super(NAME, SamlMetadataResponse::new);
    }
}
