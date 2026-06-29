/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;

/** Per-node inner request for the cache snapshot action. Carries no additional parameters. */
public class CacheSnapshotNodeRequest extends AbstractTransportRequest {

    public CacheSnapshotNodeRequest() {}

    public CacheSnapshotNodeRequest(StreamInput in) throws IOException {
        super(in);
    }
}
