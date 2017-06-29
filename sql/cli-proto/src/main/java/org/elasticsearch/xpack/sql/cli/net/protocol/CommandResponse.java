/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.Action;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.Status;

public class CommandResponse extends Response {

    public final long serverTimeQueryReceived, serverTimeResponseSent, timeSpent;
    public final String requestId;
    public final Object data;

    public CommandResponse(long serverTimeQueryReceived, long serverTimeResponseSent, String requestId, Object data) {
        super(Action.COMMAND);

        this.serverTimeQueryReceived = serverTimeQueryReceived;
        this.serverTimeResponseSent = serverTimeResponseSent;
        this.timeSpent = serverTimeQueryReceived - serverTimeResponseSent;
        this.requestId = requestId;

        this.data = data;
    }

    public void encode(DataOutput out) throws IOException {
        out.writeInt(Status.toSuccess(action));

        out.writeLong(serverTimeQueryReceived);
        out.writeLong(serverTimeResponseSent);
        out.writeUTF(requestId);
    }

    public static CommandResponse decode(DataInput in) throws IOException {
        long serverTimeQueryReceived = in.readLong();
        long serverTimeResponseSent = in.readLong();
        String requestId = in.readUTF();

        return new CommandResponse(serverTimeQueryReceived, serverTimeResponseSent, requestId, null);
    }
}