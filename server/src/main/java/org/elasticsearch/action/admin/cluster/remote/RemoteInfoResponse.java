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

package org.elasticsearch.action.admin.cluster.remote;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.RemoteConnectionInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public final class RemoteInfoResponse extends ActionResponse implements ToXContentObject {

    private List<RemoteConnectionInfo> infos;

    RemoteInfoResponse(StreamInput in) throws IOException {
        super(in);
        infos = in.readList(RemoteConnectionInfo::new);
    }

    RemoteInfoResponse(Collection<RemoteConnectionInfo> infos) {
        this.infos = List.copyOf(infos);
    }

    public List<RemoteConnectionInfo> getInfos() {
        return infos;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(infos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (RemoteConnectionInfo info : infos) {
            info.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
