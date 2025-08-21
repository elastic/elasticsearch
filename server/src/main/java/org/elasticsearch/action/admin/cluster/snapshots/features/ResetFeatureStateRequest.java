/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.features;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

/** Request for resetting feature state */
public class ResetFeatureStateRequest extends MasterNodeRequest<ResetFeatureStateRequest> {

    public static ResetFeatureStateRequest fromStream(StreamInput in) throws IOException {
        return new ResetFeatureStateRequest(in);
    }

    public ResetFeatureStateRequest(TimeValue masterNodeTimeout) {
        super(masterNodeTimeout);
    }

    private ResetFeatureStateRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

}
