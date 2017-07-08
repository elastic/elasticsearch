/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.Action;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.Status;

public class CommandResponse extends Response {

    public final long serverTimeQueryReceived, serverTimeResponseSent;
    public final String requestId;
    public final String data;

    public CommandResponse(long serverTimeQueryReceived, long serverTimeResponseSent, String requestId, String data) {
        super(Action.COMMAND);

        this.serverTimeQueryReceived = serverTimeQueryReceived;
        this.serverTimeResponseSent = serverTimeResponseSent;
        this.requestId = requestId;

        this.data = data;
    }

    public CommandResponse(DataInput in) throws IOException {
        super(Action.COMMAND);
        serverTimeQueryReceived = in.readLong();
        serverTimeResponseSent = in.readLong();
        requestId = in.readUTF();
        data = in.readUTF();
    }

    public void encode(DataOutput out) throws IOException {
        out.writeInt(Status.toSuccess(action)); // NOCOMMIT not symetric!

        out.writeLong(serverTimeQueryReceived);
        out.writeLong(serverTimeResponseSent);
        out.writeUTF(requestId);
        out.writeUTF(data);
    }

    @Override
    public String toString() {
        return "CommandResponse<received=[" + serverTimeQueryReceived 
                + "] sent=[" + serverTimeResponseSent
                + "] requestId=[" + requestId
                + "] data=[" + data + "]>";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        CommandResponse other = (CommandResponse) obj;
        return serverTimeQueryReceived == other.serverTimeQueryReceived
                && serverTimeResponseSent == other.serverTimeResponseSent
                && Objects.equals(requestId, other.requestId)
                && Objects.equals(data, other.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverTimeQueryReceived, serverTimeResponseSent, requestId, data);
    }
}