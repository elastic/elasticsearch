/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.RequestType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CommandRequest extends Request {
    public final String command;

    public CommandRequest(String command) {
        this.command = command;
    }

    CommandRequest(int clientVersion, DataInput in) throws IOException {
        command = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(command);
    }

    @Override
    protected String toStringBody() {
        return command;
    }

    @Override
    public RequestType requestType() {
        return RequestType.COMMAND;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        CommandRequest other = (CommandRequest) obj;
        return Objects.equals(command, other.command);
    }

    @Override
    public int hashCode() {
        return Objects.hash(command);
    }
}
