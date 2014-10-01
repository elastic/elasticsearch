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

package org.elasticsearch.search.reducers.bucket.slidingwindow;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.reducers.Reducer;
import org.elasticsearch.search.reducers.ReducerFactory;

import java.io.IOException;

public class SlidingWindowParser implements Reducer.Parser{

    protected static final ParseField WINDOW_SIZE_FIELD = new ParseField("window_size");
    protected static final ParseField PATH_FIELD = new ParseField("path");

    @Override
    public String type() {
        return InternalSlidingWindow.TYPE.name();
    }

    @Override
    public ReducerFactory parse(String reducerName, XContentParser parser, SearchContext context) throws IOException {
        
        String path = null;
        int windowSize = 2;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (WINDOW_SIZE_FIELD.match(currentFieldName)) {
                    windowSize = parser.intValue(true);
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (PATH_FIELD.match(currentFieldName)) {
                        path = parser.text();
                    } else {
                        throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                    }
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
            }
        }

        if (path == null) {
            throw new SearchParseException(context, "Missing [path] in sliding_window reducer [" + reducerName + "]");
        }
        return new SlidingWindowReducer.Factory(reducerName, path, windowSize);
    }

}
