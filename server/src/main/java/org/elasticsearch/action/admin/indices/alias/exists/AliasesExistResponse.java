/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.alias.exists;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class AliasesExistResponse extends ActionResponse {

    private boolean exists;

    public AliasesExistResponse(boolean exists) {
        this.exists = exists;
    }

    AliasesExistResponse(StreamInput in) throws IOException {
        super(in);
        exists = in.readBoolean();
    }

    public boolean exists() {
        return exists;
    }

    public boolean isExists() {
        return exists();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(exists);
    }
}
