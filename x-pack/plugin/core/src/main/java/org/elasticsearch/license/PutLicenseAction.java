/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.protocol.xpack.license.PutLicenseResponse;

public class PutLicenseAction extends ActionType<PutLicenseResponse> {

    public static final PutLicenseAction INSTANCE = new PutLicenseAction();
    public static final String NAME = "cluster:admin/xpack/license/put";

    private PutLicenseAction() {
        super(NAME, PutLicenseResponse::new);
    }
}
