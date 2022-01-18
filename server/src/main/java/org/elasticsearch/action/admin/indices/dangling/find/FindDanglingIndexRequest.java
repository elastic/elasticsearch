/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.dangling.find;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class FindDanglingIndexRequest extends BaseNodesRequest<FindDanglingIndexRequest> {
    private final String indexUUID;

    public FindDanglingIndexRequest(StreamInput in) throws IOException {
        super(in);
        this.indexUUID = in.readString();
    }

    public FindDanglingIndexRequest(String indexUUID) {
        super(Strings.EMPTY_ARRAY);
        this.indexUUID = indexUUID;
    }

    public String getIndexUUID() {
        return indexUUID;
    }

    @Override
    public String toString() {
        return "FindDanglingIndicesRequest{indexUUID='" + indexUUID + "'}";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(this.indexUUID);
    }
}
