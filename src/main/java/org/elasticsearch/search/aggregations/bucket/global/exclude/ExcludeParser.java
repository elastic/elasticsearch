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
package org.elasticsearch.search.aggregations.bucket.global.exclude;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class ExcludeParser implements Aggregator.Parser {

    @Override
    public String type() {
        return InternalExclude.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {
        boolean excludeQuery = false;
        final Set<String> excludeFilters = new HashSet<>();

        String currentFieldName = null;
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_OBJECT) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("exclude_filters".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            String filter = parser.text();
                            if (filter != null) {
                                excludeFilters.add(filter);
                            }
                        }
                    } else {
                        throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]:" +
                                " [" + currentFieldName + "].");
                    }
                } else if (token.isValue()) {
                    if ("exclude_query".equals(currentFieldName)) {
                        excludeQuery = parser.booleanValue();
                    } else {
                        throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName +"]:" +
                                " [" + currentFieldName + "].");
                    }
                }
            }
        }

        return new ExcludeAggregator.Factory(aggregationName, excludeQuery, excludeFilters);
    }

}
