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

/**
 * A channel used to construct bytes / builder based outputs, and send responses.
 */
public abstract class RestChannel {

    protected final RestRequest request;
    protected final boolean detailedErrorsEnabled;

    private BytesStreamOutput bytesOut;

    protected RestChannel(RestRequest request, boolean detailedErrorsEnabled) {
        this.request = request;
        this.detailedErrorsEnabled = detailedErrorsEnabled;
    }

    public XContentBuilder newBuilder() throws IOException {
        return newBuilder(request.hasContent() ? request.content() : null);
    }

    public XContentBuilder newBuilder(@Nullable BytesReference autoDetectSource) throws IOException {
        XContentType contentType = XContentType.fromRestContentType(request.param("format", request.header("Content-Type")));
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
        XContentBuilder builder = new XContentBuilder(XContentFactory.xContent(contentType), bytesOutput());
        if (request.paramAsBoolean("pretty", false)) {
            builder.prettyPrint().lfAtEnd();
        }

        builder.humanReadable(request.paramAsBoolean("human", builder.humanReadable()));

        String casing = request.param("case");
        if (casing != null && "camelCase".equals(casing)) {
            builder.fieldCaseConversion(XContentBuilder.FieldCaseConversion.CAMELCASE);
        } else {
            // we expect all REST interfaces to write results in underscore casing, so
            // no need for double casing
            builder.fieldCaseConversion(XContentBuilder.FieldCaseConversion.NONE);
        }
        return builder;
    }

    /**
     * A channel level bytes output that can be reused. It gets reset on each call to this
     * method.
     */
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

    public RestRequest request() {
        return this.request;
    }

    public boolean detailedErrorsEnabled() {
        return detailedErrorsEnabled;
    }

    public abstract void sendResponse(RestResponse response);
}