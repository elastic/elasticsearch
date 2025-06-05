/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * A transport request with an empty payload. Not really entirely empty: all transport requests include the parent task ID, a request ID,
 * and the remote address (if applicable).
 */
public final class EmptyRequest extends AbstractTransportRequest {
    public EmptyRequest() {}

    public EmptyRequest(StreamInput in) throws IOException {
        super(in);
    }
}
