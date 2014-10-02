/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.action.get;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;


public class GetLicenseRequest extends MasterNodeReadOperationRequest<GetLicenseRequest> {

    GetLicenseRequest() {
    }


    @Override
    public ActionRequestValidationException validate() {
        return null;
        /*ActionRequestValidationException validationException = null;
        if (repositories == null) {
            validationException = addValidationError("repositories is null", validationException);
        }
        return validationException;
        */
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        readLocal(in, Version.V_1_0_0_RC2);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeLocal(out, Version.V_1_0_0_RC2);
    }
}