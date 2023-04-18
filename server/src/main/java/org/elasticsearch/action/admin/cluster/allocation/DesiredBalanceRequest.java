/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class DesiredBalanceRequest extends MasterNodeReadRequest<DesiredBalanceRequest> {
    public DesiredBalanceRequest() {}

    public DesiredBalanceRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
