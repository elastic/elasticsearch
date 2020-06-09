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

package org.elasticsearch.client.indices;

import org.elasticsearch.client.TimedRequest;
import org.elasticsearch.client.Validatable;

/**
 * A request to create an data stream.
 */
public class CreateDataStreamRequest extends TimedRequest {

    private final String dataStream;

    /**
     * Constructs a new request to create an data stream with the specified name.
     */
    public CreateDataStreamRequest(String dataStream) {
        if (dataStream == null) {
            throw new IllegalArgumentException("The data stream name cannot be null.");
        }
        this.dataStream = dataStream;
    }

    /**
     * The name of the data stream to create.
     */
    public String dataStream() {
        return dataStream;
    }
}
