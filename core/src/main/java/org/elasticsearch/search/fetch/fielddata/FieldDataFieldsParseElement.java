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

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseContext;
import org.elasticsearch.search.fetch.FetchSubPhaseParseElement;
import org.elasticsearch.search.internal.SearchContext;

/**
 * Parses field name values from the {@code fielddata_fields} parameter in a
 * search request.
 * <p/>
 * <pre>
 * {
 *   "query": {...},
 *   "fielddata_fields" : ["field1", "field2"]
 * }
 * </pre>
 */
public class FieldDataFieldsParseElement extends FetchSubPhaseParseElement<FieldDataFieldsContext> {

    @Override
    protected void innerParse(XContentParser parser, FieldDataFieldsContext fieldDataFieldsContext, SearchContext searchContext) throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_ARRAY) {
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                String fieldName = parser.text();
                fieldDataFieldsContext.add(new FieldDataFieldsContext.FieldDataField(fieldName));
            }
        } else if (token == XContentParser.Token.VALUE_STRING) {
            String fieldName = parser.text();
            fieldDataFieldsContext.add(new FieldDataFieldsContext.FieldDataField(fieldName));
        } else {
            throw new IllegalStateException("Expected either a VALUE_STRING or an START_ARRAY but got " + token);
        }
    }

    @Override
    protected FetchSubPhase.ContextFactory getContextFactory() {
        return FieldDataFieldsFetchSubPhase.CONTEXT_FACTORY;
    }
}
