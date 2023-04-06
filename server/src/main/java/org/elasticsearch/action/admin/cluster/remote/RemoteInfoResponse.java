/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.remote;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.RemoteConnectionInfo;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public final class RemoteInfoResponse extends ActionResponse implements ToXContentObject {

    private final List<RemoteConnectionInfo> infos;

    RemoteInfoResponse(StreamInput in) throws IOException {
        super(in);
        infos = in.readImmutableList(RemoteConnectionInfo::new);
    }

    public RemoteInfoResponse(Collection<RemoteConnectionInfo> infos) {
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
