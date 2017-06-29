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

public class CommandRequest extends Request {

    public final String command;

    public CommandRequest(String command) {
        super(Action.COMMAND);
        this.command = command;
    }

    @Override
    public void encode(DataOutput out) throws IOException {
        out.writeInt(action.value());
        out.writeUTF(command);
    }

    public static CommandRequest decode(DataInput in) throws IOException {
        String result = in.readUTF();
        return new CommandRequest(result);
    }
}
