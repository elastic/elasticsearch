/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.TermRangeFilter;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;

import static org.elasticsearch.index.query.support.QueryParsers.wrapSmartNameFilter;

/**
 *
 */
public class ExistsFilterParser implements FilterParser {

    public static final String NAME = "exists";

    @Inject
    public ExistsFilterParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        String fieldName = null;
        String filterName = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("field".equals(currentFieldName)) {
                    fieldName = parser.text();
                } else if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[exists] filter does not support [" + currentFieldName + "]");
                }
            }
        }

        if (fieldName == null) {
            throw new QueryParsingException(parseContext.index(), "exists must be provided with a [field]");
        }

        Filter filter = null;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
            filter = smartNameFieldMappers.mapper().rangeFilter(null, null, true, true, parseContext);
        }
        if (filter == null) {
            filter = new TermRangeFilter(fieldName, null, null, true, true);
        }

        // we always cache this one, really does not change...
        filter = parseContext.cacheFilter(filter, null);

        filter = wrapSmartNameFilter(filter, smartNameFieldMappers, parseContext);
        if (filterName != null) {
            parseContext.addNamedFilter(filterName, filter);
        }
        return filter;
    }
}
