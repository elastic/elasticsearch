/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class UnfollowAction extends ActionType<AcknowledgedResponse> {

    public static final UnfollowAction INSTANCE = new UnfollowAction();
    public static final String NAME = "indices:admin/xpack/ccr/unfollow";

    private UnfollowAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest {

        private final String followerIndex;

        public Request(String followerIndex) {
            this.followerIndex = followerIndex;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            followerIndex = in.readString();
        }

        public String getFollowerIndex() {
            return followerIndex;
        }

        @Override
        public String[] indices() {
            return new String[] {followerIndex};
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException e = null;
            if (followerIndex == null) {
                e = addValidationError("follower index is missing", e);
            }
            return e;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(followerIndex);
        }
    }

}
