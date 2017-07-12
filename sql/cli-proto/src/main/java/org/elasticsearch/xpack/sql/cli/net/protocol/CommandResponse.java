/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.ResponseType;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.protocol.shared.Response;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CommandResponse extends Response {
    public final long serverTimeQueryReceived, serverTimeResponseSent;
    public final String requestId;
    public final String data;

    public CommandResponse(long serverTimeQueryReceived, long serverTimeResponseSent, String requestId, String data) {
        this.serverTimeQueryReceived = serverTimeQueryReceived;
        this.serverTimeResponseSent = serverTimeResponseSent;
        this.requestId = requestId;

        this.data = data;
    }

    CommandResponse(Request request, DataInput in) throws IOException {
        serverTimeQueryReceived = in.readLong();
        serverTimeResponseSent = in.readLong();
        requestId = in.readUTF();
        data = in.readUTF();
    }

    @Override
    protected void write(int clientVersion, DataOutput out) throws IOException {
        out.writeLong(serverTimeQueryReceived);
        out.writeLong(serverTimeResponseSent);
        out.writeUTF(requestId);
        out.writeUTF(data);
    }

    @Override
    protected String toStringBody() {
        return "received=[" + serverTimeQueryReceived 
                + "] sent=[" + serverTimeResponseSent
                + "] requestId=[" + requestId
                + "] data=[" + data + "]";
    }

    @Override
    public RequestType requestType() {
        return RequestType.COMMAND;
    }

    @Override
    public ResponseType responseType() {
        return ResponseType.COMMAND;
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
