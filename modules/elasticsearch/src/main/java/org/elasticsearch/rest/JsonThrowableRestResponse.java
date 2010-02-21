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

package org.elasticsearch.rest;

import org.elasticsearch.util.json.JsonBuilder;

import java.io.IOException;

import static org.elasticsearch.ExceptionsHelper.*;
import static org.elasticsearch.util.json.JsonBuilder.*;

/**
 * @author kimchy (Shay Banon)
 */
public class JsonThrowableRestResponse extends JsonRestResponse {

    public JsonThrowableRestResponse(RestRequest request, Throwable t) throws IOException {
        this(request, Status.INTERNAL_SERVER_ERROR, t);
    }

    public JsonThrowableRestResponse(RestRequest request, Status status, Throwable t) throws IOException {
        super(request, status, convert(request, t));
    }

    private static JsonBuilder convert(RestRequest request, Throwable t) throws IOException {
        JsonBuilder builder = jsonBuilder().prettyPrint()
                .startObject().field("error", detailedMessage(t));
        if (t != null && request.paramAsBoolean("errorTrace", false)) {
            builder.startObject("errorTrace");
            boolean first = true;
            while (t != null) {
                if (!first) {
                    builder.startObject("cause");
                }
                buildThrowable(t, builder);
                if (!first) {
                    builder.endObject();
                }
                t = t.getCause();
                first = false;
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    private static void buildThrowable(Throwable t, JsonBuilder builder) throws IOException {
        builder.field("message", t.getMessage());
        for (StackTraceElement stElement : t.getStackTrace()) {
            builder.startObject("at")
                    .field("className", stElement.getClassName())
                    .field("methodName", stElement.getMethodName());
            if (stElement.getFileName() != null) {
                builder.field("fileName", stElement.getFileName());
            }
            if (stElement.getLineNumber() >= 0) {
                builder.field("lineNumber", stElement.getLineNumber());
            }
            builder.endObject();
        }
    }
}