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
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseUtils;

import java.io.IOException;

import static org.elasticsearch.license.plugin.action.Utils.readGeneratedLicensesFrom;
import static org.elasticsearch.license.plugin.action.Utils.writeGeneratedLicensesTo;

public class PutLicenseRequest extends AcknowledgedRequest<PutLicenseRequest> {

    private ESLicenses license;

    public PutLicenseRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * Parses license from json format to an instance of {@link org.elasticsearch.license.core.ESLicenses}
     * @param licenseDefinition license definition
     */
    public PutLicenseRequest license(String licenseDefinition) {
        try {
            return license(LicenseUtils.readLicensesFromString(licenseDefinition));
        } catch (IOException e) {
            throw new ElasticsearchIllegalArgumentException("failed to parse license source", e);
        }
    }

    public PutLicenseRequest license(ESLicenses esLicenses) {
        this.license = esLicenses;
        return this;
    }

    public ESLicenses license() {
        return license;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        license = readGeneratedLicensesFrom(in);
        readTimeout(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeGeneratedLicensesTo(license, out);
        writeTimeout(out);
    }
}
