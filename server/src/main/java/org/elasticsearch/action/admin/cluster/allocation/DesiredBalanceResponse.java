/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalance;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class DesiredBalanceResponse extends ActionResponse {

    private final DesiredBalance desiredBalance;

    public DesiredBalanceResponse(DesiredBalance desiredBalance) {
        this.desiredBalance = desiredBalance;
    }

    public static DesiredBalanceResponse readFrom(StreamInput in) throws IOException {
        return new DesiredBalanceResponse(DesiredBalance.readFrom(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeWriteable(desiredBalance);
    }
}
