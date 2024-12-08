/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class GetLicenseResponse extends ActionResponse {

    private License license;

    GetLicenseResponse(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            license = License.readLicense(in);
        }
    }

    GetLicenseResponse(License license) {
        this.license = license;
    }

    public License license() {
        return license;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (license == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            license.writeTo(out);
        }
    }

}
