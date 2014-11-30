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

package org.elasticsearch.search.reducers.bucket.union;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.reducers.Reducer;
import org.elasticsearch.search.reducers.ReducerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnionParser implements Reducer.Parser{

    protected static final ParseField BUCKETS_FIELD = new ParseField("buckets");
    public static String[] TYPES = {InternalUnion.TYPE.name()};

    @Override
    public String[] types() {
        return TYPES;
    }

    @Override
    public ReducerFactory parse(String reducerName, XContentParser parser, SearchContext context) throws IOException {
        
        List<String> bucketsPaths = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (BUCKETS_FIELD.match(currentFieldName)) {
                    List<String> paths = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            paths.add(parser.text());
                        } else {
                            throw new SearchParseException(context, "Invalid type " + token + " in [" + reducerName + "]: ["
                                    + currentFieldName + "]. Array must only contain string values.");
                        }
                    }
                    bucketsPaths = paths;
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
            }
        }

        if (bucketsPaths == null) {
            throw new SearchParseException(context, "Missing [" + BUCKETS_FIELD.getPreferredName() + "] in " + TYPES[0] + " reducer [" + reducerName + "]");
        }

        return new UnionReducer.Factory(reducerName, bucketsPaths);
    }

}
