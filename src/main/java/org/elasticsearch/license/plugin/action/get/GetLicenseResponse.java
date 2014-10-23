/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.action.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.core.ESLicenses;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GetLicenseResponse extends ActionResponse {

    private List<ESLicense> licenses = new ArrayList<>();

    GetLicenseResponse() {
    }

    GetLicenseResponse(List<ESLicense> esLicenses) {
        this.licenses = esLicenses;
    }

    public List<ESLicense> licenses() {
        return licenses;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        licenses = ESLicenses.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        ESLicenses.writeTo(licenses, out);
    }

}