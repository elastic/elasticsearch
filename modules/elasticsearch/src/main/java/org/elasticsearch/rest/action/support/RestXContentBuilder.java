/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.elasticsearch.common.compress.lzf.LZF;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.CachedStreamInput;
import org.elasticsearch.common.io.stream.LZFStreamInput;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class RestXContentBuilder {

    public static XContentBuilder restContentBuilder(RestRequest request) throws IOException {
        XContentType contentType = XContentType.fromRestContentType(request.header("Content-Type"));
        if (contentType == null) {
            // try and guess it from the body, if exists
            if (request.hasContent()) {
                contentType = XContentFactory.xContentType(request.contentByteArray(), request.contentByteArrayOffset(), request.contentLength());
            }
        }
        if (contentType == null) {
            // default to JSON
            contentType = XContentType.JSON;
        }
        XContentBuilder builder = XContentFactory.contentBuilder(contentType);
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

    public static void restDocumentSource(byte[] source, XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (LZF.isCompressed(source)) {
            BytesStreamInput siBytes = new BytesStreamInput(source);
            LZFStreamInput siLzf = CachedStreamInput.cachedLzf(siBytes);
            XContentType contentType = XContentFactory.xContentType(siLzf);
            siLzf.resetToBufferStart();
            if (contentType == builder.contentType()) {
                builder.rawField("_source", siLzf);
            } else {
                XContentParser parser = XContentFactory.xContent(contentType).createParser(siLzf);
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
