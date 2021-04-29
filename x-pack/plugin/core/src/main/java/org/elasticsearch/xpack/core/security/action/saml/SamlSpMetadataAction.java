/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.action.ActionType;

public class SamlSpMetadataAction extends ActionType<SamlSpMetadataResponse> {
    public static final String NAME = "cluster:monitor/xpack/security/saml/metadata";
    public static final SamlSpMetadataAction INSTANCE = new SamlSpMetadataAction();

    private SamlSpMetadataAction() {
        super(NAME, SamlSpMetadataResponse::new);
    }
}
