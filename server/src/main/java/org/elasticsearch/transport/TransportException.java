/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class TransportException extends ElasticsearchException {
    public TransportException(Throwable cause) {
        super(cause);
    }

    public TransportException(StreamInput in) throws IOException {
        super(in);
    }

    public TransportException(String msg) {
        super(msg);
    }

    public TransportException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
