/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

public class PutLicenseRequest extends AcknowledgedRequest<PutLicenseRequest> {

    private License license;
    private boolean acknowledge = false;

    public PutLicenseRequest(StreamInput in) throws IOException {
        super(in);
        license = License.readLicense(in);
        acknowledge = in.readBoolean();
    }

    public PutLicenseRequest() {}

    @Override
    public ActionRequestValidationException validate() {
        return (license == null) ? ValidateActions.addValidationError("license is missing", null) : null;
    }

    /**
     * Parses license from json format to an instance of {@link License}
     *
     * @param licenseDefinition licenses definition
     * @param xContentType the content type of the license
     */
    public PutLicenseRequest license(BytesReference licenseDefinition, XContentType xContentType) {
        try {
            return license(License.fromSource(licenseDefinition, xContentType));
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse license source", e);
        }
    }

    public PutLicenseRequest license(License license) {
        this.license = license;
        return this;
    }

    public License license() {
        return license;
    }

    public PutLicenseRequest acknowledge(boolean acknowledge) {
        this.acknowledge = acknowledge;
        return this;
    }

    public boolean acknowledged() {
        return acknowledge;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        license.writeTo(out);
        out.writeBoolean(acknowledge);
    }
}
