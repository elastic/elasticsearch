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

import org.elasticsearch.common.unit.TimeValue;

/**
 *
 */
public class TransportRequestOptions {

    public static final TransportRequestOptions EMPTY = options();

    public static TransportRequestOptions options() {
        return new TransportRequestOptions();
    }

    public static enum Type {
        RECOVERY,
        BULK,
        REG,
        STATE,
        PING;

        public static Type fromString(String type) {
            if ("bulk".equalsIgnoreCase(type)) {
                return BULK;
            } else if ("reg".equalsIgnoreCase(type)) {
                return REG;
            } else if ("state".equalsIgnoreCase(type)) {
                return STATE;
            } else if ("recovery".equalsIgnoreCase(type)) {
                return RECOVERY;
            } else if ("ping".equalsIgnoreCase(type)) {
                return PING;
            } else {
                throw new IllegalArgumentException("failed to match transport type for [" + type + "]");
            }
        }
    }

    private TimeValue timeout;

    private boolean compress;

    private Type type = Type.REG;

    public TransportRequestOptions withTimeout(long timeout) {
        return withTimeout(TimeValue.timeValueMillis(timeout));
    }

    public TransportRequestOptions withTimeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    public TransportRequestOptions withCompress(boolean compress) {
        this.compress = compress;
        return this;
    }

    public TransportRequestOptions withType(Type type) {
        this.type = type;
        return this;
    }

    public TimeValue timeout() {
        return this.timeout;
    }

    public boolean compress() {
        return this.compress;
    }

    public Type type() {
        return this.type;
    }
}
