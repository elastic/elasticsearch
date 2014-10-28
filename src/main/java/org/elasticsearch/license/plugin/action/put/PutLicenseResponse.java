/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.action.put;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.license.plugin.core.LicensesStatus;

import java.io.IOException;

public class PutLicenseResponse extends AcknowledgedResponse {

    private LicensesStatus status;

    PutLicenseResponse() {
    }

    PutLicenseResponse(boolean acknowledged, LicensesStatus status) {
        super(acknowledged);
        this.status = status;
    }

    public LicensesStatus status() {
        return status;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        readAcknowledged(in);
        status = LicensesStatus.fromId(in.readVInt());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeAcknowledged(out);
        out.writeVInt(status.id());
    }

}
