/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.license;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class GetLicenseResponse extends ActionResponse {

    private String license;

    public GetLicenseResponse(StreamInput in) throws IOException {
        super(in);
    }

    public GetLicenseResponse(String license) {
        this.license = license;
    }

    public String getLicenseDefinition() {
        return license;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

}
