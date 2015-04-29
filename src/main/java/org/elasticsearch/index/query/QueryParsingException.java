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

package org.elasticsearch.index.query;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexException;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 *
 */
public class QueryParsingException extends IndexException {

    static final int UNKNOWN_POSITION = -1;
    private int lineNumber = UNKNOWN_POSITION;
    private int columnNumber = UNKNOWN_POSITION;

    public QueryParsingException(QueryParseContext parseContext, String msg) {
        this(parseContext, msg, null);
    }

    public QueryParsingException(QueryParseContext parseContext, String msg, Throwable cause) {
        super(parseContext.index(), msg, cause);

        XContentParser parser = parseContext.parser();
        if (parser != null) {
            XContentLocation location = parser.getTokenLocation();
            if (location != null) {
                lineNumber = location.lineNumber;
                columnNumber = location.columnNumber;
            }
        }
    }

    /**
     * This constructor is provided for use in unit tests where a
     * {@link QueryParseContext} may not be available
     */
    QueryParsingException(Index index, int line, int col, String msg, Throwable cause) {
        super(index, msg, cause);
        this.lineNumber = line;
        this.columnNumber = col;
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

}
