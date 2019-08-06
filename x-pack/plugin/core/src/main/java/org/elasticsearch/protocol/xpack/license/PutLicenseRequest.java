/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.license;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class PutLicenseRequest extends AcknowledgedRequest<PutLicenseRequest> {

    private String licenseDefinition;
    private boolean acknowledge = false;

    public PutLicenseRequest(StreamInput in) throws IOException {
        super(in);

    }

    public PutLicenseRequest() {

    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public void setLicenseDefinition(String licenseDefinition) {
        this.licenseDefinition = licenseDefinition;
    }

    public String getLicenseDefinition() {
        return licenseDefinition;
    }

    public void setAcknowledge(boolean acknowledge) {
        this.acknowledge = acknowledge;
    }

    public boolean isAcknowledge() {
        return acknowledge;
    }
}
