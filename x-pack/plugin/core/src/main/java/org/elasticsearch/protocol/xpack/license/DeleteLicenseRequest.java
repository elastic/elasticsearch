/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.license;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;


public class DeleteLicenseRequest extends AcknowledgedRequest<DeleteLicenseRequest> {

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
