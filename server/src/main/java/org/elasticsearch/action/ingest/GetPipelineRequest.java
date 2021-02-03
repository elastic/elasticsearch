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

    public GetPipelineRequest(String... ids) {
        if (ids == null) {
            throw new IllegalArgumentException("ids cannot be null");
        }
        this.ids = ids;
    }

    GetPipelineRequest() {
        this.ids = Strings.EMPTY_ARRAY;
    }

    public GetPipelineRequest(StreamInput in) throws IOException {
        super(in);
        ids = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(ids);
    }

    public String[] getIds() {
        return ids;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
