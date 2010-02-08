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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilterClause;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Lists.*;

/**
 * @author kimchy (Shay Banon)
 */
public class BoolJsonFilterParser extends AbstractIndexComponent implements JsonFilterParser {

    @Inject public BoolJsonFilterParser(Index index, @IndexSettings Settings settings) {
        super(index, settings);
    }

    @Override public String name() {
        return "bool";
    }

    @Override public Filter parse(JsonQueryParseContext parseContext) throws IOException, QueryParsingException {
        JsonParser jp = parseContext.jp();

        List<FilterClause> clauses = newArrayList();

        String currentFieldName = null;
        JsonToken token;
        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                currentFieldName = jp.getCurrentName();
            } else if (token == JsonToken.START_OBJECT) {
                if ("must".equals(currentFieldName)) {
                    clauses.add(new FilterClause(parseContext.parseInnerFilter(), BooleanClause.Occur.MUST));
                } else if ("mustNot".equals(currentFieldName)) {
                    clauses.add(new FilterClause(parseContext.parseInnerFilter(), BooleanClause.Occur.MUST_NOT));
                } else if ("should".equals(currentFieldName)) {
                    clauses.add(new FilterClause(parseContext.parseInnerFilter(), BooleanClause.Occur.SHOULD));
                }
            }
        }

        BooleanFilter booleanFilter = new BooleanFilter();
        for (FilterClause filterClause : clauses) {
            booleanFilter.add(filterClause);
        }
        // no need to cache this one, inner queries will be cached and thats good enough (I think...)
        return booleanFilter;
    }
}