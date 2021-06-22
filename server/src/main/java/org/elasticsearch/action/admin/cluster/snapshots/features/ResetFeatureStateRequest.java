/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.features;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/** Request for resetting feature state */
public class ResetFeatureStateRequest extends MasterNodeRequest<ResetFeatureStateRequest> {

    private static final Version FEATURE_RESET_ON_MASTER = Version.V_8_0_0;

    public static ResetFeatureStateRequest fromStream(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(FEATURE_RESET_ON_MASTER)) {
            return new ResetFeatureStateRequest(in);
        } else {
            LegacyResetFeatureStateRequest legacyResetFeatureStateRequest = new LegacyResetFeatureStateRequest(in);
            return legacyResetFeatureStateRequest.toResetFeatureStateRequest();
        }
    }

    public ResetFeatureStateRequest() {}

    private ResetFeatureStateRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(FEATURE_RESET_ON_MASTER)) {
            super.writeTo(out);
        } else {
            this.toLegacyStateRequest().writeTo(out);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    private LegacyResetFeatureStateRequest toLegacyStateRequest() {
        LegacyResetFeatureStateRequest legacyResetFeatureStateRequest = new LegacyResetFeatureStateRequest();
        legacyResetFeatureStateRequest.setParentTask(this.getParentTask());
        return legacyResetFeatureStateRequest;
    }

    private static class LegacyResetFeatureStateRequest extends ActionRequest {

        private LegacyResetFeatureStateRequest() {}

        private LegacyResetFeatureStateRequest(StreamInput in) throws IOException {
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

        ResetFeatureStateRequest toResetFeatureStateRequest() {
            ResetFeatureStateRequest request = new ResetFeatureStateRequest();
            request.setParentTask(this.getParentTask());
            return request;
        }
    }
}
