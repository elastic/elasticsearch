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

package org.elasticsearch.rest.action.support;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;

/**
 *
 */
public class RestXContentBuilder {

    public static XContentBuilder restContentBuilder(RestRequest request) throws IOException {
        // use the request body as the auto detect source (if it exists)
        return restContentBuilder(request, request.hasContent() ? request.content() : null);
    }

    public static XContentBuilder restContentBuilder(RestRequest request, @Nullable BytesReference autoDetectSource) throws IOException {
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
        XContentBuilder builder = new XContentBuilder(XContentFactory.xContent(contentType), new BytesStreamOutput());
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
     * Directly writes the source to the output builder
     */
    public static void directSource(BytesReference source, XContentBuilder rawBuilder, ToXContent.Params params) throws IOException {
        XContentHelper.writeDirect(source, rawBuilder, params);
    }

    public static void restDocumentSource(BytesReference source, XContentBuilder builder, ToXContent.Params params) throws IOException {
        XContentHelper.writeRawField("_source", source, builder, params);
    }
}
