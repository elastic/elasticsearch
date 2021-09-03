/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Thrown by {@link Diff#apply} method
 */
public class IncompatibleClusterStateVersionException extends ElasticsearchException {
    public IncompatibleClusterStateVersionException(String msg) {
        super(msg);
    }

    public IncompatibleClusterStateVersionException(long expectedVersion, String expectedUuid, long receivedVersion, String receivedUuid) {
        super("Expected diff for version " + expectedVersion + " with uuid " + expectedUuid + " got version " +
            receivedVersion + " and uuid " + receivedUuid);
    }

    public IncompatibleClusterStateVersionException(StreamInput in) throws IOException{
        super(in);
    }
}
