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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class SearchParseException extends SearchContextException {

    public static final int UNKNOWN_POSITION = -1;
    private final int lineNumber;
    private final int columnNumber;

    public SearchParseException(SearchContext context, String msg, @Nullable XContentLocation location) {
        this(context, msg, location, null);
    }

    public SearchParseException(SearchContext context, String msg, @Nullable XContentLocation location, Throwable cause) {
        super(context, msg, cause);
        int lineNumber = UNKNOWN_POSITION;
        int columnNumber = UNKNOWN_POSITION;
        if (location != null) {
            if (location != null) {
                lineNumber = location.lineNumber;
                columnNumber = location.columnNumber;
            }
        }
        this.columnNumber = columnNumber;
        this.lineNumber = lineNumber;
    }

    public SearchParseException(StreamInput in) throws IOException {
        super(in);
        lineNumber = in.readInt();
        columnNumber = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(lineNumber);
        out.writeInt(columnNumber);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

    @Override
    protected void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        if (lineNumber != UNKNOWN_POSITION) {
            builder.field("line", lineNumber);
            builder.field("col", columnNumber);
        }
        super.innerToXContent(builder, params);
    }

    /**
     * Line number of the location of the error
     * 
     * @return the line number or -1 if unknown
     */
    public int getLineNumber() {
        return lineNumber;
    }

    /**
     * Column number of the location of the error
     * 
     * @return the column number or -1 if unknown
     */
    public int getColumnNumber() {
        return columnNumber;
    }
}
