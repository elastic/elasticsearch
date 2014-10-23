/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.action.put;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.core.ESLicenses;

import java.io.IOException;
import java.util.List;


public class PutLicenseRequest extends AcknowledgedRequest<PutLicenseRequest> {

    private List<ESLicense> licenses;

    public PutLicenseRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * Parses licenses from json format to an instance of {@link org.elasticsearch.license.core.ESLicenses}
     * @param licenseDefinition licenses definition
     */
    public PutLicenseRequest licenses(String licenseDefinition) {
        try {
            return licenses(ESLicenses.fromSource(licenseDefinition));
        } catch (IOException e) {
            throw new ElasticsearchIllegalArgumentException("failed to parse licenses source", e);
        }
    }

    public PutLicenseRequest licenses(List<ESLicense> esLicenses) {
        this.licenses = esLicenses;
        return this;
    }

    public List<ESLicense> licenses() {
        return licenses;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        licenses = ESLicenses.readFrom(in);
        readTimeout(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        ESLicenses.writeTo(licenses, out);
        writeTimeout(out);
    }
}
