/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.protocol.shared;

import org.elasticsearch.xpack.sql.protocol.shared.AbstractProto.RequestType;

import java.io.DataOutput;
import java.io.IOException;

public abstract class Request {
    @Override
    public final String toString() {
        return getClass().getSimpleName() + "<" + toStringBody() + ">";
    }

    /**
     * Write this request to the {@link DataOutput}. Implementers should
     * be kind and stick this right under the ctor that reads the response.
     */
    protected abstract void write(DataOutput out) throws IOException;

    /**
     * Body to go into the {@link #toString()} result.
     */
    protected abstract String toStringBody();

    /**
     * Type of this request.
     */
    public abstract RequestType requestType();

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
