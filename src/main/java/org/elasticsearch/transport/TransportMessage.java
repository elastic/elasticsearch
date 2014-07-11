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

import com.google.common.collect.Maps;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.transport.TransportAddress;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public abstract class TransportMessage<TM extends TransportMessage<TM>> implements Streamable {

    private Map<String, Object> headers;

    private TransportAddress remoteAddress;

    protected TransportMessage() {
    }

    protected TransportMessage(TM message) {
        // create a new copy of the headers, since we are creating a new request which might have
        // its headers changed in the context of that specific request
        if (message.getHeaders() != null) {
            this.headers = new HashMap<>(message.getHeaders());
        }
    }


    public void remoteAddress(TransportAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    public TransportAddress remoteAddress() {
        return remoteAddress;
    }

    @SuppressWarnings("unchecked")
    public final TM putHeader(String key, Object value) {
        if (headers == null) {
            headers = Maps.newHashMap();
        }
        headers.put(key, value);
        return (TM) this;
    }

    @SuppressWarnings("unchecked")
    public final <V> V getHeader(String key) {
        if (headers == null) {
            return null;
        }
        return (V) headers.get(key);
    }

    public Map<String, Object> getHeaders() {
        return this.headers;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            headers = in.readMap();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (headers == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeMap(headers);
        }
    }
}
