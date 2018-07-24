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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public interface TransportResponseHandler<T extends TransportResponse> extends Writeable.Reader<T> {

    /**
     * @deprecated Implement {@link #read(StreamInput)} instead.
     */
    @Deprecated
    default T newInstance() {
        throw new UnsupportedOperationException();
    }

    /**
     * deserializes a new instance of the return type from the stream.
     * called by the infra when de-serializing the response.
     *
     * @return the deserialized response.
     */
    @Override
    default T read(StreamInput in) throws IOException {
        T instance = newInstance();
        instance.readFrom(in);
        return instance;
    }

    void handleResponse(T response);

    void handleException(TransportException exp);

    String executor();
}
