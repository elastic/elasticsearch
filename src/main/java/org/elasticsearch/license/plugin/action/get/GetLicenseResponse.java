/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.action.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.plugin.cluster.LicensesMetaData;

import java.io.IOException;

import static org.elasticsearch.license.plugin.action.Utils.readLicensesFrom;
import static org.elasticsearch.license.plugin.action.Utils.writeLicensesTo;

public class GetLicenseResponse extends ActionResponse {

    private ESLicenses licenses = null;

    GetLicenseResponse() {
    }

    GetLicenseResponse(ESLicenses esLicenses) {
        this.licenses = esLicenses;
    }

    public ESLicenses licenses() {
        return licenses;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        licenses = readLicensesFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeLicensesTo(licenses, out);
    }

}