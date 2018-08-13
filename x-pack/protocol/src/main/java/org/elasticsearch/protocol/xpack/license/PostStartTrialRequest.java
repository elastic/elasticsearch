/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.protocol.xpack.license;

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
