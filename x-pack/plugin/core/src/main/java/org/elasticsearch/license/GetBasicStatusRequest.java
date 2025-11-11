/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV10;

import java.io.IOException;

public class GetBasicStatusRequest extends LocalClusterStateRequest {

    public GetBasicStatusRequest(TimeValue masterNodeTimeout) {
        super(masterNodeTimeout);
    }

    /**
     * Prior to 9.3 TransportGetBasicStatusAction was a TransportMasterNodeReadAction so for BwC we need to be
     * able to read this from the transport layer until we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DISTRIBUTED_COORDINATION)
    public GetBasicStatusRequest(StreamInput in) throws IOException {
        super(in);
    }
}
