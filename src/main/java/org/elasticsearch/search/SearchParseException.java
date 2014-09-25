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

package org.elasticsearch.search;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * We only offer constructors that takes XContentLocation parameter to encourage
 * developers of parsers to pass detailed information back with all exceptions.
 */
public class SearchParseException extends SearchContextException implements ToXContent{

    private ToXContent xContentExplanation;

    public SearchParseException(SearchContext context, String msg, @Nullable XContentLocation location) {
        this(context, msg, null, location);
    }


    public SearchParseException(SearchContext context, String msg, Throwable cause, @Nullable XContentLocation location) {
        super(context, "Parse Failure [" + msg + "]", cause);
        if (location != null) {
            xContentExplanation = new ParseErrorDetails(msg, location.getLineNumber(), location.getColumnNumber());
        } else {
            if (cause instanceof ToXContent) {
                xContentExplanation = (ToXContent) cause;
            }
        }
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }


    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (xContentExplanation != null) {
            xContentExplanation.toXContent(builder, params);
            return builder;
        }
        return builder;
    }

    
}
