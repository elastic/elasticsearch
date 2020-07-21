/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.reelect;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Response returned after a cluster reelect request
 */
public class ClusterReelectResponse extends AcknowledgedResponse implements ToXContentObject {

    private String message;

    ClusterReelectResponse(StreamInput in) throws IOException {
        super(in);
        message = in.readOptionalString();
    }

    ClusterReelectResponse(boolean acknowledged) {
        super(acknowledged);
    }

    ClusterReelectResponse(boolean acknowledged, String message) {
        super(acknowledged);
        this.message = message;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(message);
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        if (message != null && !message.isEmpty()) {
            builder.field("message", message);
        }
    }
}
