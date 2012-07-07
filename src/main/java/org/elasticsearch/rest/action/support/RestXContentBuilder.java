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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedStreamInput;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.CachedStreamOutput;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;

/**
 *
 */
public class RestXContentBuilder {

    public static XContentBuilder restContentBuilder(RestRequest request) throws IOException {
        XContentType contentType = XContentType.fromRestContentType(request.header("Content-Type"));
        if (contentType == null) {
            // try and guess it from the body, if exists
            if (request.hasContent()) {
                contentType = XContentFactory.xContentType(request.content());
            }
        }
        if (contentType == null) {
            // default to JSON
            contentType = XContentType.JSON;
        }
        CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
        XContentBuilder builder = new XContentBuilder(XContentFactory.xContent(contentType), cachedEntry.bytes(), cachedEntry);
        if (request.paramAsBoolean("pretty", false)) {
            builder.prettyPrint();
        }
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

    public static void restDocumentSource(BytesReference source, XContentBuilder builder, ToXContent.Params params) throws IOException {
        Compressor compressor = CompressorFactory.compressor(source);
        if (compressor != null) {
            CompressedStreamInput compressedStreamInput = compressor.streamInput(source.streamInput());
            XContentType contentType = XContentFactory.xContentType(compressedStreamInput);
            compressedStreamInput.resetToBufferStart();
            if (contentType == builder.contentType()) {
                builder.rawField("_source", compressedStreamInput);
            } else {
                XContentParser parser = XContentFactory.xContent(contentType).createParser(compressedStreamInput);
                try {
                    parser.nextToken();
                    builder.field("_source");
                    builder.copyCurrentStructure(parser);
                } finally {
                    parser.close();
                }
            }
        } else {
            XContentType contentType = XContentFactory.xContentType(source);
            if (contentType == builder.contentType()) {
                builder.rawField("_source", source);
            } else {
                XContentParser parser = XContentFactory.xContent(contentType).createParser(source);
                try {
                    parser.nextToken();
                    builder.field("_source");
                    builder.copyCurrentStructure(parser);
                } finally {
                    parser.close();
                }
            }
        }
    }
}
