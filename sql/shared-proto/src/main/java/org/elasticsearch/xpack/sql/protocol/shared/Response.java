/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.protocol.shared;

import org.elasticsearch.xpack.sql.protocol.shared.AbstractProto.RequestType;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractProto.ResponseType;

import java.io.DataOutput;
import java.io.IOException;

public abstract class Response {
    @Override
    public final String toString() {
        return getClass().getSimpleName() + "<" + toStringBody() + ">";
    }

    /**
     * Write this response to the {@link DataOutput}.
     * @param clientVersion The version of the client that requested
     *      the message. This should be used to send a response compatible
     *      with the client.
     */
    protected abstract void write(int clientVersion, DataOutput out) throws IOException;

    /**
     * Body to go into the {@link #toString()} result.
     */
    protected abstract String toStringBody();

    /**
     * Type of the request for which this is the response.
     */
    public abstract RequestType requestType();

    /**
     * Type of this response.
     */
    public abstract ResponseType responseType();

    /*
     * Must properly implement {@linkplain #equals(Object)} for
     * round trip testing.
     */
    @Override
    public abstract boolean equals(Object obj);

    /*
     * Must properly implement {@linkplain #hashCode()} for
     * round trip testing.
     */
    @Override
    public abstract int hashCode();
}
