/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;

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
