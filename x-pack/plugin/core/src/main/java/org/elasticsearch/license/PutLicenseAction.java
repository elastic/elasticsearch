/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.Action;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.protocol.xpack.license.PutLicenseResponse;

public class PutLicenseAction extends Action<PutLicenseResponse> {

    public static final PutLicenseAction INSTANCE = new PutLicenseAction();
    public static final String NAME = "cluster:admin/xpack/license/put";

    private PutLicenseAction() {
        super(NAME);
    }

    @Override
    public PutLicenseResponse newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public Writeable.Reader<PutLicenseResponse> getResponseReader() {
        return PutLicenseResponse::new;
    }
}
