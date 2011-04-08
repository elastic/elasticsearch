/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.query.xcontent;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.PublicTermsFilter;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;

import static org.elasticsearch.index.query.support.QueryParsers.*;

/**
 * @author kimchy (shay.banon)
 */
public class TermsFilterParser extends AbstractIndexComponent implements XContentFilterParser {

    public static final String NAME = "terms";

    @Inject public TermsFilterParser(Index index, @IndexSettings Settings settings) {
        super(index, settings);
    }

    @Override public String[] names() {
        return new String[]{NAME, "in"};
    }

    @Override public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        MapperService.SmartNameFieldMappers smartNameFieldMappers = null;
        boolean cache = true;
        PublicTermsFilter termsFilter = new PublicTermsFilter();
        String filterName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                String fieldName = currentFieldName;
                FieldMapper fieldMapper = null;
                smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
                if (smartNameFieldMappers != null) {
                    if (smartNameFieldMappers.hasMapper()) {
                        fieldMapper = smartNameFieldMappers.mapper();
                        fieldName = fieldMapper.names().indexName();
                    }
                }

                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    String value = parser.text();
                    if (value == null) {
                        throw new QueryParsingException(index, "No value specified for term filter");
                    }
                    if (fieldMapper != null) {
                        value = fieldMapper.indexedValue(value);
                    }
                    termsFilter.addTerm(new Term(fieldName, value));
                }
            } else if (token.isValue()) {
                if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else if ("_cache".equals(currentFieldName)) {
                    cache = parser.booleanValue();
                }
            }
        }

        Filter filter = termsFilter;
        if (cache) {
            filter = parseContext.cacheFilter(filter);
        }

        filter = wrapSmartNameFilter(filter, smartNameFieldMappers, parseContext);
        if (filterName != null) {
            parseContext.addNamedFilter(filterName, filter);
        }
        return filter;
    }
}
