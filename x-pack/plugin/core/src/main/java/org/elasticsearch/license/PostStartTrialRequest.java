/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class PostStartTrialRequest extends MasterNodeRequest<PostStartTrialRequest> {

    private boolean acknowledge = false;
    private String type;

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public PostStartTrialRequest setType(String type) {
        this.type = type;
        return this;
    }

    public String getType() {
        return type;
    }

    public PostStartTrialRequest acknowledge(boolean acknowledge) {
        this.acknowledge = acknowledge;
        return this;
    }

    public boolean isAcknowledged() {
        return acknowledge;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.getVersion().onOrAfter(Version.V_6_3_0)) {
            type = in.readString();
            acknowledge = in.readBoolean();
        } else {
            type = "trial";
            acknowledge = true;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Version version = Version.V_6_3_0;
        if (out.getVersion().onOrAfter(version)) {
            super.writeTo(out);
            out.writeString(type);
            out.writeBoolean(acknowledge);
        } else {
            if ("trial".equals(type) == false) {
                throw new IllegalArgumentException("All nodes in cluster must be version [" + version
                        + "] or newer to start trial with a different type than 'trial'. Attempting to write to " +
                        "a node with version [" + out.getVersion() + "] with trial type [" + type + "].");
            } else if (acknowledge == false) {
                throw new IllegalArgumentException("Request must be acknowledged to send to a node with a version " +
                        "prior to [" + version + "]. Attempting to send request to node with version [" + out.getVersion() + "] " +
                        "without acknowledgement.");
            } else {
                super.writeTo(out);
            }
        }
    }
}
