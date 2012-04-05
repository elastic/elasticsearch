/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.ElasticSearchIllegalArgumentException;
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
        LOW,
        MED,
        HIGH;

        public static Type fromString(String type) {
            if ("low".equalsIgnoreCase(type)) {
                return LOW;
            } else if ("med".equalsIgnoreCase(type)) {
                return MED;
            } else if ("high".equalsIgnoreCase(type)) {
                return HIGH;
            } else {
                throw new ElasticSearchIllegalArgumentException("failed to match transport type for [" + type + "]");
            }
        }
    }

    private TimeValue timeout;

    private boolean compress;

    private Type type = Type.MED;

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

    /**
     * A request that requires very low latency. Usually reserved for ping requests with very small payload.
     */
    public TransportRequestOptions withHighType() {
        this.type = Type.HIGH;
        return this;
    }

    /**
     * The typical requests flows go through this one.
     */
    public TransportRequestOptions withMedType() {
        this.type = Type.MED;
        return this;
    }

    /**
     * Batch oriented (big payload) based requests use this one.
     */
    public TransportRequestOptions withLowType() {
        this.type = Type.LOW;
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
