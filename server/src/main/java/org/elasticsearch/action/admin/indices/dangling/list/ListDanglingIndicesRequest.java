/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.dangling.list;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class ListDanglingIndicesRequest extends BaseNodesRequest<ListDanglingIndicesRequest> {
    /**
     * Filter the response by index UUID. Leave as null to find all indices.
     */
    private final String indexUUID;

    public ListDanglingIndicesRequest(StreamInput in) throws IOException {
        super(in);
        this.indexUUID = in.readOptionalString();
    }

    public ListDanglingIndicesRequest() {
        super(Strings.EMPTY_ARRAY);
        this.indexUUID = null;
    }

    public ListDanglingIndicesRequest(String indexUUID) {
        super(Strings.EMPTY_ARRAY);
        this.indexUUID = indexUUID;
    }

    public String getIndexUUID() {
        return indexUUID;
    }

    @Override
    public String toString() {
        return "ListDanglingIndicesRequest{indexUUID='" + indexUUID + "'}";
    }
}
