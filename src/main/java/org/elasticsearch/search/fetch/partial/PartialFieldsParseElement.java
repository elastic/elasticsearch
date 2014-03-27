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

package org.elasticsearch.search.fetch.partial;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.List;

/**
 * <pre>
 * "partial_fields" : {
 *  "test1" : {
 *      "includes" : "doc['field_name'].value"
 *  },
 *  "test2" : {
 *      "excludes" : "..."
 *  }
 * }
 * </pre>
 */
public class PartialFieldsParseElement implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                String fieldName = currentFieldName;
                List<String> includes = null;
                List<String> excludes = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        if ("includes".equals(currentFieldName) || "include".equals(currentFieldName)) {
                            if (includes == null) {
                                includes = new ArrayList<>(2);
                            }
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                includes.add(parser.text());
                            }
                        } else if ("excludes".equals(currentFieldName) || "exclude".equals(currentFieldName)) {
                            if (excludes == null) {
                                excludes = new ArrayList<>(2);
                            }
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                excludes.add(parser.text());
                            }
                        }
                    } else if (token.isValue()) {
                        if ("include".equals(currentFieldName)) {
                            if (includes == null) {
                                includes = new ArrayList<>(2);
                            }
                            includes.add(parser.text());
                        } else if ("exclude".equals(currentFieldName)) {
                            if (excludes == null) {
                                excludes = new ArrayList<>(2);
                            }
                            excludes.add(parser.text());
                        }
                    }
                }
                PartialFieldsContext.PartialField field = new PartialFieldsContext.PartialField(fieldName,
                        includes == null ? Strings.EMPTY_ARRAY : includes.toArray(new String[includes.size()]),
                        excludes == null ? Strings.EMPTY_ARRAY : excludes.toArray(new String[excludes.size()]));
                context.partialFields().add(field);
            }
        }
    }
}