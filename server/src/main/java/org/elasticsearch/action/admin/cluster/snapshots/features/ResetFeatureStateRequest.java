/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.features;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/** Request for resetting feature state */
public class ResetFeatureStateRequest extends MasterNodeRequest<ResetFeatureStateRequest> {

    private static final Version FEATURE_RESET_ON_MASTER = Version.V_7_14_0;

    public static ResetFeatureStateRequest fromStream(StreamInput in) throws IOException {
        if (in.getVersion().before(FEATURE_RESET_ON_MASTER)) {
            throw new IllegalStateException(
                "feature reset is not available in a cluster that have nodes with version before " + FEATURE_RESET_ON_MASTER
            );
        }
        return new ResetFeatureStateRequest(in);
    }

    public ResetFeatureStateRequest() {}

    private ResetFeatureStateRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().before(FEATURE_RESET_ON_MASTER)) {
            throw new IllegalStateException(
                "feature reset is not available in a cluster that have nodes with version before " + FEATURE_RESET_ON_MASTER
            );
        }
        super.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

}
