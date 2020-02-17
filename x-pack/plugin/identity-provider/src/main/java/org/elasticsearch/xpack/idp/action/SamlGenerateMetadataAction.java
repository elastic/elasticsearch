/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionType;

public class SamlGenerateMetadataAction extends ActionType<SamlGenerateMetadataResponse> {

    public static final String NAME = "cluster:admin/idp/saml/metadata";
    public static final SamlGenerateMetadataAction INSTANCE = new SamlGenerateMetadataAction();

    private SamlGenerateMetadataAction() {
        super(NAME, SamlGenerateMetadataResponse::new);
    }
}
