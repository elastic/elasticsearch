/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.Action;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InfoRequest extends Request {
    public InfoRequest() {
        super(Action.INFO);
    }

    @Override
    public void encode(DataOutput out) throws IOException {
        out.writeInt(action.value());
    }

    public static InfoRequest decode(DataInput in) throws IOException {
        return new InfoRequest();
    }
}
