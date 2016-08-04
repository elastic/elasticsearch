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
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public abstract class AbstractRestChannel implements RestChannel {

    protected final RestRequest request;
    protected final boolean detailedErrorsEnabled;

    private BytesStreamOutput bytesOut;

    protected AbstractRestChannel(RestRequest request, boolean detailedErrorsEnabled) {
        this.request = request;
        this.detailedErrorsEnabled = detailedErrorsEnabled;
    }

    @Override
    public XContentBuilder newBuilder() throws IOException {
        return newBuilder(request.hasContent() ? request.content() : null, request.hasParam("filter_path"));
    }

    @Override
    public XContentBuilder newErrorBuilder() throws IOException {
        // Disable filtering when building error responses
        return newBuilder(request.hasContent() ? request.content() : null, false);
    }

    @Override
    public XContentBuilder newBuilder(@Nullable BytesReference autoDetectSource, boolean useFiltering) throws IOException {
        XContentType contentType = XContentType.fromMediaTypeOrFormat(request.param("format", request.header("Accept")));
        if (contentType == null) {
            // try and guess it from the auto detect source
            if (autoDetectSource != null) {
                contentType = XContentFactory.xContentType(autoDetectSource);
            }
        }
        if (contentType == null) {
            // default to JSON
            contentType = XContentType.JSON;
        }

        String[] filters = useFiltering ? request.paramAsStringArrayOrEmptyIfAll("filter_path") :  null;
        XContentBuilder builder = new XContentBuilder(XContentFactory.xContent(contentType), bytesOutput(), filters);
        if (request.paramAsBoolean("pretty", false)) {
            builder.prettyPrint().lfAtEnd();
        }

        builder.humanReadable(request.paramAsBoolean("human", builder.humanReadable()));
        return builder;
    }

    /**
     * A channel level bytes output that can be reused. It gets reset on each call to this
     * method.
     */
    @Override
    public final BytesStreamOutput bytesOutput() {
        if (bytesOut == null) {
            bytesOut = newBytesOutput();
        } else {
            bytesOut.reset();
        }
        return bytesOut;
    }

    protected BytesStreamOutput newBytesOutput() {
        return new BytesStreamOutput();
    }

    @Override
    public RestRequest request() {
        return this.request;
    }

    @Override
    public boolean detailedErrorsEnabled() {
        return detailedErrorsEnabled;
    }

}
