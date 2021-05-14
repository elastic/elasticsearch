/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class GetPipelineRequest extends MasterNodeReadRequest<GetPipelineRequest> {

    private String[] ids;
    private final boolean summary;

    public GetPipelineRequest(boolean summary, String... ids) {
        if (ids == null) {
            throw new IllegalArgumentException("ids cannot be null");
        }
        this.ids = ids;
        this.summary = summary;
    }

    public GetPipelineRequest(String... ids) {
        this(false, ids);
    }

    GetPipelineRequest() {
        this(false, Strings.EMPTY_ARRAY);
    }

    public GetPipelineRequest(StreamInput in) throws IOException {
        super(in);
        ids = in.readStringArray();
        summary = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(ids);
        out.writeBoolean(summary);
    }

    public String[] getIds() {
        return ids;
    }

    public boolean isSummary() {
        return summary;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
