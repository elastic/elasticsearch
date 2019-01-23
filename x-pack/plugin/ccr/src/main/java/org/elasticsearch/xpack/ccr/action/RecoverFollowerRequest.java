/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

class RecoverFollowerRequest extends MasterNodeRequest<RecoverFollowerRequest> {

    private final String remoteCluster;
    private final String leaderIndex;
    private final String followerIndex;

    RecoverFollowerRequest(String remoteCluster, String leaderIndex, String followerIndex) {
        this.remoteCluster = remoteCluster;
        this.leaderIndex = leaderIndex;
        this.followerIndex = followerIndex;
    }

    RecoverFollowerRequest(StreamInput input) throws IOException {
        super(input);
        remoteCluster = input.readString();
        leaderIndex = input.readString();
        followerIndex = input.readString();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getRemoteCluster() {
        return remoteCluster;
    }

    String getFollowerIndex() {
        return followerIndex;
    }

    String getLeaderIndex() {
        return leaderIndex;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(remoteCluster);
        out.writeString(leaderIndex);
        out.writeString(followerIndex);
    }
}
