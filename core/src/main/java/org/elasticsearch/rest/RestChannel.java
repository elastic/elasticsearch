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

package org.elasticsearch.rest;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A channel used to construct bytes / builder based outputs, and send responses.
 */
public interface RestChannel {

    XContentBuilder newBuilder() throws IOException;

    XContentBuilder newErrorBuilder() throws IOException;

    XContentBuilder newBuilder(@Nullable BytesReference autoDetectSource, boolean useFiltering) throws IOException;

    BytesStreamOutput bytesOutput();

    RestRequest request();

    /**
     * @return true iff an error response should contain additional details like exception traces.
     */
    boolean detailedErrorsEnabled();

    void sendResponse(RestResponse response);

}
