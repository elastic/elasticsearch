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

package org.elasticsearch.index.query.json;

import com.google.inject.Inject;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.TermsFilter;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

import static org.elasticsearch.index.query.support.QueryParsers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class TermsJsonFilterParser extends AbstractIndexComponent implements JsonFilterParser {

    public static final String NAME = "terms";

    @Inject public TermsJsonFilterParser(Index index, @IndexSettings Settings settings) {
        super(index, settings);
    }

    @Override public String name() {
        return NAME;
    }

    @Override public Filter parse(JsonQueryParseContext parseContext) throws IOException, QueryParsingException {
        JsonParser jp = parseContext.jp();

        JsonToken token = jp.nextToken();
        assert token == JsonToken.FIELD_NAME;
        String fieldName = jp.getCurrentName();

        FieldMapper fieldMapper = null;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null) {
            if (smartNameFieldMappers.hasMapper()) {
                fieldMapper = smartNameFieldMappers.mapper();
                fieldName = fieldMapper.names().indexName();
            }
        }

        token = jp.nextToken();
        if (token != JsonToken.START_ARRAY) {
            throw new QueryParsingException(index, "Terms filter must define the terms to filter on as an array");
        }

        TermsFilter termsFilter = new TermsFilter();
        while ((token = jp.nextToken()) != JsonToken.END_ARRAY) {
            String value = jp.getText();
            if (value == null) {
                throw new QueryParsingException(index, "No value specified for term filter");
            }
            if (fieldMapper != null) {
                value = fieldMapper.indexedValue(value);
            }
            termsFilter.addTerm(new Term(fieldName, value));
        }
        jp.nextToken();


        Filter filter = parseContext.cacheFilterIfPossible(termsFilter);
        return wrapSmartNameFilter(filter, smartNameFieldMappers, parseContext.filterCache());
    }
}
