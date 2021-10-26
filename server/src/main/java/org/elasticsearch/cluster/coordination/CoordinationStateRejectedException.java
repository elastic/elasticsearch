/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * This exception is thrown when rejecting state transitions on the {@link CoordinationState} object,
 * for example when receiving a publish request with the wrong term or version.
 * Occurrences of this exception don't always signal failures, but can often be just caused by the
 * asynchronous, distributed nature of the system. They will, for example, naturally happen during
 * leader election, if multiple nodes are trying to become leader at the same time.
 */
public class CoordinationStateRejectedException extends ElasticsearchException {
    public CoordinationStateRejectedException(String msg, Object... args) {
        super(msg, args);
    }

    public CoordinationStateRejectedException(StreamInput in) throws IOException {
        super(in);
    }
}
