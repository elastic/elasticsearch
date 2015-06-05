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

package org.elasticsearch.search.fetch.source;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * <pre>
 * "source" : true/false
 * "source" : "field"
 * "source" : [ "include", "include" ]
 * "source" : {
 *     "include" : ["obj"]
 *     "exclude" : ["obj"]
 * }
 * </pre>
 */
public class FetchSourceParseElement implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        context.fetchSourceContext(parse(parser));
    }

    public FetchSourceContext parse(XContentParser parser) throws IOException {
        XContentParser.Token token;

        List<String> includes = null, excludes = null;
        String currentFieldName = null;
        token = parser.currentToken(); // we get it on the value
        if (parser.isBooleanValue()) {
            return new FetchSourceContext(parser.booleanValue());
        } else if (token == XContentParser.Token.VALUE_STRING) {
            return new FetchSourceContext(new String[]{parser.text()});
        } else if (token == XContentParser.Token.START_ARRAY) {
            includes = new ArrayList<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                includes.add(parser.text());
            }
        } else if (token == XContentParser.Token.START_OBJECT) {

            List<String> currentList = null;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                    if ("includes".equals(currentFieldName) || "include".equals(currentFieldName)) {
                        currentList = includes != null ? includes : (includes = new ArrayList<>(2));
                    } else if ("excludes".equals(currentFieldName) || "exclude".equals(currentFieldName)) {
                        currentList = excludes != null ? excludes : (excludes = new ArrayList<>(2));
                    } else {
                        throw new ElasticsearchParseException("Source definition may not contain " + parser.text());
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        currentList.add(parser.text());
                    }
                } else if (token.isValue()) {
                    currentList.add(parser.text());
                } else {
                    throw new ElasticsearchParseException("unexpected token while parsing source settings");
                }
            }
        } else {
            throw new ElasticsearchParseException("source element value can be of type " + token.name());
        }

        return new FetchSourceContext(
                includes == null ? Strings.EMPTY_ARRAY : includes.toArray(new String[includes.size()]),
                excludes == null ? Strings.EMPTY_ARRAY : excludes.toArray(new String[excludes.size()]));
    }
}
