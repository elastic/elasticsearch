/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

public class TransportRequestOptions {

    @Nullable
    private final TimeValue timeout;
    private final Type type;

    public static TransportRequestOptions timeout(@Nullable TimeValue timeout) {
        return of(timeout, Type.REG);
    }

    public static TransportRequestOptions of(@Nullable TimeValue timeout, Type type) {
        if (timeout == null && type == Type.REG) {
            return EMPTY;
        }
        return new TransportRequestOptions(timeout, type);
    }

    private TransportRequestOptions(@Nullable TimeValue timeout, Type type) {
        this.timeout = timeout;
        this.type = type;
    }

    @Nullable
    public TimeValue timeout() {
        return this.timeout;
    }

    public Type type() {
        return this.type;
    }

    public static final TransportRequestOptions EMPTY = new TransportRequestOptions(null, Type.REG);

    public enum Type {
        RECOVERY,
        BULK,
        REG,
        STATE,
        PING
    }
}
