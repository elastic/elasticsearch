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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.TermRangeFilter;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.NotFilter;
import org.elasticsearch.common.lucene.search.XBooleanFilter;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;

import static org.elasticsearch.index.query.support.QueryParsers.wrapSmartNameFilter;

/**
 *
 */
public class MissingFilterParser implements FilterParser {

    public static final String NAME = "missing";

    @Inject
    public MissingFilterParser() {
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
        boolean nullValue = false;
        boolean existence = true;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("field".equals(currentFieldName)) {
                    fieldName = parser.text();
                } else if ("null_value".equals(currentFieldName)) {
                    nullValue = parser.booleanValue();
                } else if ("existence".equals(currentFieldName)) {
                    existence = parser.booleanValue();
                } else if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[missing] filter does not support [" + currentFieldName + "]");
                }
            }
        }

        if (fieldName == null) {
            throw new QueryParsingException(parseContext.index(), "missing must be provided with a [field]");
        }

        if (!existence && !nullValue) {
            throw new QueryParsingException(parseContext.index(), "missing must have either existence, or null_value, or both set to true");
        }

        Filter existenceFilter = null;
        Filter nullFilter = null;


        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);

        if (existence) {
            if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
                existenceFilter = smartNameFieldMappers.mapper().rangeFilter(null, null, true, true, parseContext);
            }
            if (existenceFilter == null) {
                existenceFilter = new TermRangeFilter(fieldName, null, null, true, true);
            }

            // we always cache this one, really does not change... (exists)
            existenceFilter = parseContext.cacheFilter(existenceFilter, null);
            existenceFilter = new NotFilter(existenceFilter);
            // cache the not filter as well, so it will be faster
            existenceFilter = parseContext.cacheFilter(existenceFilter, null);
        }

        if (nullValue) {
            if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
                nullFilter = smartNameFieldMappers.mapper().nullValueFilter();
                if (nullFilter != null) {
                    // cache the not filter as well, so it will be faster
                    nullFilter = parseContext.cacheFilter(nullFilter, null);
                }
            }
        }

        Filter filter;
        if (nullFilter != null) {
            if (existenceFilter != null) {
                XBooleanFilter combined = new XBooleanFilter();
                combined.add(existenceFilter, BooleanClause.Occur.SHOULD);
                combined.add(nullFilter, BooleanClause.Occur.SHOULD);
                // cache the not filter as well, so it will be faster
                filter = parseContext.cacheFilter(combined, null);
            } else {
                filter = nullFilter;
            }
        } else {
            filter = existenceFilter;
        }

        if (filter == null) {
            return null;
        }

        filter = wrapSmartNameFilter(filter, smartNameFieldMappers, parseContext);
        if (filterName != null) {
            parseContext.addNamedFilter(filterName, existenceFilter);
        }
        return filter;
    }
}
