/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import java.io.DataOutput;
import java.io.IOException;

import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.Action;

abstract class Message {

    public final Action action;

    protected Message(Action action) {
        this.action = action;
    }

    @Override
    public String toString() {
        return action.name();
    }

    public abstract void encode(DataOutput out) throws IOException;
}