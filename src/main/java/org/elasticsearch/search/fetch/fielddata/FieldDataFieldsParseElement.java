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
package org.elasticsearch.search.fetch.fielddata;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

/**
 * Parses field name values from the {@code fielddata_fields} parameter in a
 * search request.
 *
 * <pre>
 * {
 *   "query": {...},
 *   "fielddata_fields" : ["field1", "field2"]
 * }
 * </pre>
 */
public class FieldDataFieldsParseElement implements SearchParseElement {
    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_ARRAY) {
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                String fieldName = parser.text();
                context.fieldDataFields().add(new FieldDataFieldsContext.FieldDataField(fieldName));
            }
        } else if (token == XContentParser.Token.VALUE_STRING) {
            String fieldName = parser.text();
            context.fieldDataFields().add(new FieldDataFieldsContext.FieldDataField(fieldName));
        }  else {
            throw new ElasticsearchIllegalStateException("Expected either a VALUE_STRING or an START_ARRAY but got " + token);
        }
    }
}
