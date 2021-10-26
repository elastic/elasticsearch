/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.enrollment;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public final class NodeEnrollmentRequest extends ActionRequest {

    public NodeEnrollmentRequest() {
    }

    public NodeEnrollmentRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override public ActionRequestValidationException validate() {
        return null;
    }
}
