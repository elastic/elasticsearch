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

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.util.xcontent.XContentFactory;
import org.elasticsearch.util.xcontent.XContentType;
import org.elasticsearch.util.xcontent.builder.BinaryXContentBuilder;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class RestXContentBuilder {

    public static BinaryXContentBuilder restContentBuilder(RestRequest request) throws IOException {
        XContentType contentType = XContentType.fromRestContentType(request.header("Content-Type"));
        if (contentType == null) {
            // try and guess it from the body, if exists
            if (request.hasContent()) {
                contentType = XContentFactory.xContentType(request.contentAsBytes());
            }
        }
        if (contentType == null) {
            // default to JSON
            contentType = XContentType.JSON;
        }
        BinaryXContentBuilder builder = XContentFactory.contentBinaryBuilder(contentType);
        if (request.paramAsBoolean("pretty", false)) {
            builder.prettyPrint();
        }
        return builder;
    }
}
