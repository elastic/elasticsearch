/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.seektracker;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class SeekStatsRequest extends BaseNodesRequest<SeekStatsRequest> {

    private final String[] indices;

    public SeekStatsRequest(String... indices) {
        super(Strings.EMPTY_ARRAY);
        this.indices = indices;
    }

    public SeekStatsRequest(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
    }

    public String[] getIndices() {
        return indices;
    }

}
