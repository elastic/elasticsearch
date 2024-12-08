/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.protocol.xpack.frozen;

import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class FreezeResponse extends OpenIndexResponse {
    public FreezeResponse(StreamInput in) throws IOException {
        super(in);
    }

    public FreezeResponse(boolean acknowledged, boolean shardsAcknowledged) {
        super(acknowledged, shardsAcknowledged);
    }
}
